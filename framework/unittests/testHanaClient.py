
import unittest

from framework.unittests.doubles.HanaMock import HanaConnectionMock

from framework.validation.hana.HanaClient import HanaClient, HanaConnectionData


class testHanaClient(unittest.TestCase):

    def test_basics(self):
        cut = HanaClient()
        hanamock = HanaConnectionMock()
        cut._connection = hanamock

        hanamock.set_sql_result([[234]])
        count = cut.get_rowcount('SYSTEM', 'ZCDS_SNWD_SO_2')
        self.assertEqual(234, count)
        self.assertEqual(
            'SELECT "RECORD_COUNT" FROM "SYS"."M_TABLES" WHERE SCHEMA_NAME = \'SYSTEM\' AND TABLE_NAME = \'ZCDS_SNWD_SO_2\'', hanamock.last_sql)

        hanamock.set_sql_result([[13]])
        count = cut.get_rowcount('TESTSCHEMA', 'TESTTABLE')
        self.assertEqual(13, count)
        self.assertEqual(
            'SELECT "RECORD_COUNT" FROM "SYS"."M_TABLES" WHERE SCHEMA_NAME = \'TESTSCHEMA\' AND TABLE_NAME = \'TESTTABLE\'', hanamock.last_sql)
        records = [("DIBUG1", "HIGH"), ("DIBUG2", "MEDIUM"), ("DIBUG3", "LOW")]
        hanamock.set_sql_result(records)
        result = cut.get_rows_by_offset("CIT_TEST", "TESTTABLE", 3, 0)
        self.assertEqual(records, result)
        records = [("DIBUG1", "HIGH"), ("DIBUG2", "MEDIUM"), ("DIBUG3", "LOW")]
        hanamock.set_sql_result(records)
        result = cut.get_rows_by_offset(
            "CIT_TEST", "TESTTABLE", 3, 0, "DIBUG2", True)
        self.assertEqual(records, result)

    def test_drop_table(self):
        cut = HanaClient()

        hanamock = HanaConnectionMock()
        cut._connection = hanamock

        cut.drop_table('SYSTEM', 'ZCDS_SNWD_SO_2')
        self.assertEqual(
            'DROP TABLE SYSTEM.ZCDS_SNWD_SO_2', hanamock.last_sql)

    def test_table_exists(self):
        cut = HanaClient()
        hanamock = HanaConnectionMock()
        cut._connection = hanamock

        records = [("some-schema", "some-table", "TABLE")]
        hanamock.set_sql_result(records)
        exists = cut.table_exists('some-schema', 'some-table')
        self.assertEqual(
            'SELECT * FROM OBJECTS WHERE OBJECT_TYPE=\'TABLE\' AND SCHEMA_NAME=\'some-schema\' AND OBJECT_NAME=\'some-table\'', hanamock.last_sql)
        self.assertTrue(exists)

        records = []
        hanamock.set_sql_result(records)
        exists = cut.table_exists('some-schema', 'some-table')
        self.assertEqual(
            'SELECT * FROM OBJECTS WHERE OBJECT_TYPE=\'TABLE\' AND SCHEMA_NAME=\'some-schema\' AND OBJECT_NAME=\'some-table\'', hanamock.last_sql)
        self.assertFalse(exists)

    def test_create_schema(self):
        cut = HanaClient()
        hanamock = HanaConnectionMock()
        cut._connection = hanamock

        cut.create_schema("some-schema")
        self.assertEqual(
            'CREATE SCHEMA "some-schema"', hanamock.last_sql)

    def test_delte_schema(self):
        cut = HanaClient()
        hanamock = HanaConnectionMock()
        cut._connection = hanamock

        cut.delete_schema("some-schema")
        self.assertEqual(
            'DROP SCHEMA "some-schema" CASCADE', hanamock.last_sql)

    def test_schema_exists(self):
        cut = HanaClient()
        hanamock = HanaConnectionMock()
        cut._connection = hanamock

        records = [("some-schema", "DBADMIN", "TRUE")]
        hanamock.set_sql_result(records)
        exists = cut.schema_exists("some-schema")
        self.assertEqual(
            'SELECT * FROM "SYS"."SCHEMAS" WHERE (SCHEMA_NAME = \'some-schema\')', hanamock.last_sql)
        self.assertTrue(exists)

        records = []
        hanamock.set_sql_result(records)
        exists = cut.schema_exists("some-schema")
        self.assertEqual(
            'SELECT * FROM "SYS"."SCHEMAS" WHERE (SCHEMA_NAME = \'some-schema\')', hanamock.last_sql)
        self.assertFalse(exists)


class testHanaConnectionData(unittest.TestCase):

    def test_basics(self):
        cut = HanaConnectionData('HANA_EU10')
        self.assertEqual('HANA_EU10', cut.name)

    def test_properties(self):
        cut = HanaConnectionData('HANA_EU10')
        cut.address = 'host.eu10.sap.com'
        self.assertEqual('host.eu10.sap.com', cut.address)
        cut.port = '443'
        self.assertEqual('443', cut.port)
        cut.user = 'DBADMIN'
        self.assertEqual('DBADMIN', cut.user)
        cut.password = "123456"
        self.assertEqual('123456', cut.password)
