from multiprocessing import connection
import os
import unittest
from framework.infrastructure.Utils import ConnectionData

from framework.validation.datalake.DatalakeClient import DatalakeClient, DatalakeConnectionData


class testDatalakeClientBasics(unittest.TestCase):

    @unittest.skip
    # Makes sure to provide a user and password for a user on datalake system below
    def test_ConnectionToDatalake(self):
        datalakeClient = DatalakeClient.connect_to(accountname='', accountkey='',
                                                   container='dataintegration2021')
        files = datalakeClient.get_fileNames("CIT_TEST/TestInitialLoad")
        self.assertEqual(1601, len(files))
    @unittest.skip
    # Makes sure to provide a user and password for a user on datalake system below
    def test_connection_to_datalake_with_connectiondata(self):
        connectionData = DatalakeConnectionData("datalake")
        connectionData.accountname = ''
        connectionData.accountkey = ''
        connectionData.container = 'dataintegration2021'
        foldername = "CIT_TEST/TestInitialLoad"
        datalakeClient = DatalakeClient.connect_to(connectionData)
        isExsited = datalakeClient.is_file_Exist(
            foldername, 'part-004300.json')
        self.assertEqual(False, isExsited)
        isExsited = datalakeClient.is_file_Exist(
            foldername, 'part-000001.json')
        self.assertEqual(True, isExsited)
        fileContent = datalakeClient.get_jsonfile_content(
            foldername, 'part-000001.json')
        expectContent = {'FIELD': {'COLUMNNAME': 'NodeKey', 'KEY': 'X', 'ABAPTYPE': 'RAW',
                                   'ABAPLEN': '000016', 'OUTPUTLEN': '000032', 'DECIMALS': '000000'}}
        self.assertEqual(expectContent, fileContent["METADATA"][0])

    def test_connection_to_datalake_with_connectiondata_from_file(self):
        # Make sure that you provide the credentials either in DataLake.secrets.json or
        # have them in environment variables:
        # DataLake_accountkey =
        test_path = os.path.join(os.path.dirname(
            __file__), 'testdata', 'connectiondata')
        connection_data = ConnectionData.for_datalake('DataLake', test_path)
        datalakeClient = DatalakeClient.connect_to(connection_data)
        files = datalakeClient.get_fileNames("CIT_TEST/TestInitialLoad")
        self.assertEqual(1601, len(files))
