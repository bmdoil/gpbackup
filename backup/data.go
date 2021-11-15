package backup

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/jackc/pgconn"
	"gopkg.in/cheggaaa/pb.v1"
)

var (
	tableDelim = ","
)

func ConstructTableAttributesList(columnDefs []ColumnDefinition) string {
	names := make([]string, 0)
	for _, col := range columnDefs {
		names = append(names, col.Name)
	}
	if len(names) > 0 {
		return fmt.Sprintf("(%s)", strings.Join(names, ","))
	}
	return ""
}

func AddTableDataEntriesToTOC(tables []Table, rowsCopiedMaps []map[uint32]int64) {
	for _, table := range tables {
		if !table.SkipDataBackup() {
			var rowsCopied int64
			for _, rowsCopiedMap := range rowsCopiedMaps {
				if val, ok := rowsCopiedMap[table.Oid]; ok {
					rowsCopied = val
					break
				}
			}
			attributes := ConstructTableAttributesList(table.ColumnDefs)
			globalTOC.AddMasterDataEntry(table.Schema, table.Name, table.Oid, attributes, rowsCopied, table.PartitionLevelInfo.RootName)
		}
	}
}

type BackupProgressCounters struct {
	NumRegTables   int64
	TotalRegTables int64
	ProgressBar    utils.ProgressBar
}

func CopyTableOut(connectionPool *dbconn.DBConn, table Table, destinationToWrite string, connNum int) (int64, error) {
	checkPipeExistsCommand := ""
	customPipeThroughCommand := utils.GetPipeThroughProgram().OutputCommand
	sendToDestinationCommand := ">"
	if MustGetFlagBool(options.SINGLE_DATA_FILE) {
		/*
		 * The segment TOC files are always written to the segment data directory for
		 * performance reasons, in case the user-specified directory is on a mounted
		 * drive.  It will be copied to a user-specified directory, if any, once all
		 * of the data is backed up.
		 */
		checkPipeExistsCommand = fmt.Sprintf("(test -p \"%s\" || (echo \"Pipe not found %s\">&2; exit 1)) && ", destinationToWrite, destinationToWrite)
		customPipeThroughCommand = "cat -"
	} else if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		sendToDestinationCommand = fmt.Sprintf("| %s backup_data %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath)
	}

	copyCommand := fmt.Sprintf("PROGRAM '%s%s %s %s'", checkPipeExistsCommand, customPipeThroughCommand, sendToDestinationCommand, destinationToWrite)

	query := fmt.Sprintf("COPY %s TO %s WITH CSV DELIMITER '%s' ON SEGMENT IGNORE EXTERNAL PARTITIONS;", table.FQN(), copyCommand, tableDelim)
	gplog.Verbose("Worker %d: %s", connNum, query)
	result, err := connectionPool.Exec(query, connNum)
	if err != nil {
		return 0, err
	}
	numRows, _ := result.RowsAffected()
	return numRows, nil
}

func BackupSingleTableData(table Table, rowsCopiedMap map[uint32]int64, counters *BackupProgressCounters, whichConn int) error {
	atomic.AddInt64(&counters.NumRegTables, 1)
	numTables := counters.NumRegTables //We save this so it won't be modified before we log it
	if gplog.GetVerbosity() > gplog.LOGINFO {
		// No progress bar at this log level, so we note table count here
		gplog.Verbose("Writing data for table %s to file (table %d of %d)", table.FQN(), numTables, counters.TotalRegTables)
	} else {
		gplog.Verbose("Writing data for table %s to file", table.FQN())
	}

	destinationToWrite := ""
	if MustGetFlagBool(options.SINGLE_DATA_FILE) {
		destinationToWrite = fmt.Sprintf("%s_%d", globalFPInfo.GetSegmentPipePathForCopyCommand(), table.Oid)
	} else {
		destinationToWrite = globalFPInfo.GetTableBackupFilePathForCopyCommand(table.Oid, utils.GetPipeThroughProgram().Extension, false)
	}
	rowsCopied, err := CopyTableOut(connectionPool, table, destinationToWrite, whichConn)
	if err != nil {
		return err
	}
	rowsCopiedMap[table.Oid] = rowsCopied
	counters.ProgressBar.Increment()
	return nil
}
type safeOIDMap struct {
	mutex  *sync.RWMutex
	oidMap map[uint32]int
}

func (s *safeOIDMap) Init(tables []Table) {
	s.mutex = &sync.RWMutex{}
	s.oidMap = make(map[uint32]int, len(tables))
	for _, table := range tables {
		s.oidMap[table.Oid] = Unknown
	}
}
func (s *safeOIDMap) SafeSet(key uint32, value int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.oidMap[key] = value
}

func (s *safeOIDMap) SafeGet(key uint32) int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.oidMap[key]
}

func (s *safeOIDMap) List() map[uint32]int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.oidMap
}

func backupDataForAllTables(tables []Table) []map[uint32]int64 {
	var numExtOrForeignTables int64
	for _, table := range tables {
		if table.SkipDataBackup() {
			numExtOrForeignTables++
		}
	}
	counters := BackupProgressCounters{NumRegTables: 0, TotalRegTables: int64(len(tables)) - numExtOrForeignTables}
	counters.ProgressBar = utils.NewProgressBar(int(counters.TotalRegTables), "Tables backed up: ", utils.PB_INFO)
	counters.ProgressBar.Start()
	rowsCopiedMaps := make([]map[uint32]int64, connectionPool.NumConns)
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to kill any COPY statements
	 * in progress if they don't finish on their own.
	 */
	tasks := make(chan Table, len(tables))
	// Record hashmap of oids to track which have been processed
	var oidMap safeOIDMap
	oidMap.Init(tables)
	var workerPool sync.WaitGroup
	var copyErr error
	// Loading the tables as tasks. Start goroutines that work on the tasks
	for _, table := range tables {
		tasks <- table
	}
	// We incremented numConns by 1 to treat connNum 0 as a special worker
	rowsCopiedMaps[0] = make(map[uint32]int64)
	for connNum := 1; connNum < connectionPool.NumConns; connNum++ {
		rowsCopiedMaps[connNum] = make(map[uint32]int64)
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()
			for table := range tasks {
				if wasTerminated || copyErr != nil {
					counters.ProgressBar.(*pb.ProgressBar).NotPrint = true
					return
				}

				if table.SkipDataBackup() {
					gplog.Verbose("Skipping data backup of table %s because it is either an external or foreign table.", table.FQN())
					oidMap.SafeSet(table.Oid, Complete)
					continue
				}
				// If a random external SQL command had queued an AccessExclusiveLock acquisition request
				// against this next table, the --job worker thread would deadlock on the COPY attempt.
				// To prevent gpbackup from hanging, we attempt to acquire an AccessShareLock on the
				// relation with the NOWAIT option before we run COPY. If the LOCK TABLE NOWAIT call
				// fails, we catch the error and defer the table to the main worker thread, worker 0.
				// Afterwards, we break early and terminate the worker since its transaction is now in an
				// aborted state. We do not need to do this with the main worker thread because it has
				// already acquired AccessShareLocks on all tables before the metadata dumping part.

				err := LockTableNoWait(table, whichConn)
				if err != nil {
					if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code != PG_LOCK_NOT_AVAILABLE {
						copyErr = err
						continue
					}

					if gplog.GetVerbosity() < gplog.LOGVERBOSE {
						// Add a newline to interrupt the progress bar so that
						// the following WARN message is nicely outputted.
						fmt.Printf("\n")
					}
					gplog.Warn("Worker %d could not acquire AccessShareLock for table %s. Terminating worker and deferring table to main worker thread.",
						whichConn, table.FQN())

					oidMap.SafeSet(table.Oid, Deferred)

					// Rollback transaction since it's in an aborted state
					connectionPool.MustRollback(whichConn)

					// Worker no longer has a valid distributed transaction snapshot
					break
				}

				err = BackupSingleTableData(table, rowsCopiedMaps[whichConn], &counters, whichConn)
				if err != nil {
					copyErr = err
				}
				oidMap.SafeSet(table.Oid, Complete)

			}
		}(connNum)
	}

	// Special goroutine to handle deferred tables
	// Handle all tables deferred by the deadlock detection. This can only be
	// done with the main worker thread, worker 0, because it has
	// AccessShareLocks on all the tables already.
	deferredWorkerDone := make(chan bool)
	go func() {
		for _, table := range tables {
			for {
				switch oidMap.SafeGet(table.Oid) {
				case Unknown:
					continue
				case Deferred:
					err := BackupSingleTableData(table, rowsCopiedMaps[0], &counters, 0)
					if err != nil {
						copyErr = err
					}
					oidMap.SafeSet(table.Oid, Complete)
				}
				if oidMap.SafeGet(table.Oid) == Complete {
					break
				}
			}
		}
		deferredWorkerDone <- true
	}()

	close(tasks)
	workerPool.Wait()

	allWorkersTerminatedLogged := false
	for oid := range oidMap.List() {
		if oidMap.SafeGet(oid) == Unknown {
			if !allWorkersTerminatedLogged {
				gplog.Warn("All parallel workers terminated due to lock issues. Falling back to single main worker.")
				allWorkersTerminatedLogged = true
			}
			oidMap.SafeSet(oid, Deferred) // All deferred tables are processed by worker 0
		}
	}
	<-deferredWorkerDone

	var agentErr error
	if MustGetFlagBool(options.SINGLE_DATA_FILE) {
		agentErr = utils.CheckAgentErrorsOnSegments(globalCluster, globalFPInfo)
	}

	if copyErr != nil && agentErr != nil {
		gplog.Error(agentErr.Error())
		gplog.Fatal(copyErr, "")
	} else if copyErr != nil {
		gplog.Fatal(copyErr, "")
	} else if agentErr != nil {
		gplog.Fatal(agentErr, "")
	}

	counters.ProgressBar.Finish()
	printDataBackupWarnings(numExtOrForeignTables)
	return rowsCopiedMaps
}

func printDataBackupWarnings(numExtTables int64) {
	if numExtTables > 0 {
		gplog.Info("Skipped data backup of %d external/foreign table(s).", numExtTables)
		gplog.Info("See %s for a complete list of skipped tables.", gplog.GetLogFilePath())
	}
}

func CheckTablesContainData(tables []Table) {
	if !backupReport.MetadataOnly {
		for _, table := range tables {
			if !table.SkipDataBackup() {
				return
			}
		}
		gplog.Warn("No tables in backup set contain data. Performing metadata-only backup instead.")
		backupReport.MetadataOnly = true
	}
}

// Acquire AccessShareLock on a table with NOWAIT option. If we are unable to acquire
// the lock, the call will fail instead of block. Return the failure for handling.
func LockTableNoWait(dataTable Table, connNum int) error {
	query := fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE NOWAIT;", dataTable.FQN())
	gplog.Verbose("Worker %d: %s", connNum, query)
	_, err := connectionPool.Exec(query, connNum)
	if err != nil {
		return err
	}

	return nil
}
