import os
import unittest
from framework.infrastructure.Utils import ConnectionData

from framework.validation.hana.HanaClient import HanaClient, HanaConnectionData


class testHanaClientBasics(unittest.TestCase):

    @unittest.skip
    # Makes sure to provide a user and password for a user on Hana system below
    def test_connectionToHana(self):
        cut = HanaClient.connect_to(
            address='87cc15ab-65ef-4b28-9433-962f7400aecc.hana.canary-eu10.hanacloud.ondemand.com', port=443, user='', password='')
        count = cut.get_rowcount('CIT_TEST', 'GEN2_SNWD_SLT_I308555')
        self.assertEqual(8000100, count)

    @unittest.skip
    # Makes sure to provide a user and password for a user on Hana system below
    def test_connection_to_hana_with_connectiondata(self):
        connectionData = HanaConnectionData("HANA_EU10")
        connectionData.address = '87cc15ab-65ef-4b28-9433-962f7400aecc.hana.canary-eu10.hanacloud.ondemand.com'
        connectionData.port = 443
        connectionData.user = ''
        connectionData.password = ''
        cut = HanaClient.connect_to(connectionData)
        count = cut.get_rowcount('CIT_TEST', 'GEN2_SNWD_SLT_I308555')
        self.assertEqual(8000100, count)

    def test_connection_to_hana_with_connectiondata_from_file(self):
        test_path = os.path.join(os.path.dirname(
            __file__), 'testdata', 'connectiondata')
        connection_data = ConnectionData.for_hana('HANA_EU10', test_path)
        cut = HanaClient.connect_to(connection_data)
        count = cut.get_rowcount('CIT_TEST', 'GEN2_SNWD_SLT_I308555')
        self.assertEqual(8000100, count)

    @unittest.skip
    def test_create_and_delete_schema(self):
        test_path = os.path.join(os.path.dirname(
            __file__), 'testdata', 'connectiondata')
        connection_data = ConnectionData.for_hana('HANA_EU10', test_path)
        cut = HanaClient.connect_to(connection_data)
        schema_name = 'arbitrary_test_schema'
        self.assertFalse(cut.schema_exists(schema_name))
        cut.create_schema(schema_name)
        self.assertTrue(cut.schema_exists(schema_name))
        cut.delete_schema(schema_name)
        self.assertFalse(cut.schema_exists(schema_name))

    @unittest.skip
    def test_table_exists(self):
        test_path = os.path.join(os.path.dirname(
            __file__), 'testdata', 'connectiondata')
        connection_data = ConnectionData.for_hana('HANA_EU10', test_path)
        cut = HanaClient.connect_to(connection_data)
        schema_name = 'E2E-TEST-AUTOMATION'
        table_name = 'TESTEXECUTION'
        self.assertTrue(cut.table_exists(schema_name, table_name))
        self.assertFalse(cut.table_exists(schema_name, 'foo'))
