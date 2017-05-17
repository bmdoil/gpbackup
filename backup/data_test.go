package backup_test

import (
	"database/sql"
	"gpbackup/backup"
	"gpbackup/testutils"
	"gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("backup/data tests", func() {
	var connection *utils.DBConn
	var mock sqlmock.Sqlmock
	BeforeEach(func() {
		connection, mock = testutils.CreateAndConnectMockDB()
		testutils.SetupTestLogger()
	})
	Describe("CopyTableOut", func() {
		It("will dump a table to its own file", func() {
			testTable := utils.Relation{2345, 3456, "public", "foo", sql.NullString{"", false}}
			execStr := "COPY public.foo TO '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;"
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			backup.CopyTableOut(connection, testTable, filename)
		})
	})
	Describe("CreateTableDumpPath", func() {
		It("will create the dump path for data", func() {
			testTable := utils.Relation{2345, 3456, "public", "foo", sql.NullString{"", false}}
			utils.DumpTimestamp = "20170101010101"
			expectedFilename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			actualFilename := backup.CreateTableDumpPath(testTable)
			Expect(actualFilename).To(Equal(expectedFilename))
		})
	})
})
