import os
import unittest
from framework.infrastructure.Utils import ConnectionData

from framework.validation.abap.AbapClient import AbapConnectionData
from framework.validation.abap.CitAbapClient import CitAbapClient


def get_connection_data():
    # Make sure that you provide the credentials either in S4H_2021.secrets.json or
    # have them in environment variables:
    # S4H_2021_USER =
    # S4H_2021_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'testdata', 'connectiondata')

    connection_data = ConnectionData.for_abap('S4H_2021', test_path)

    return connection_data


class testCitAbapClientBasics(unittest.TestCase):

    @unittest.skip
    # Makes sure to provide a user and password for a user on S4H_2021 below
    def test_ConnectionToAbap(self):
        cut = CitAbapClient.connect_to(user='', passwd='',
                                       ashost='52.166.148.139', sysnr='00', client='100')
        count = cut.get_rowcount('WN_LS')
        self.assertEqual(100, count)

    @unittest.skip
    # Makes sure to provide a user and password for a user on S4H_2021 below
    def test_connection_to_abap_with_connectiondata(self):
        connection_data = AbapConnectionData('QOI')
        connection_data.user = ''
        connection_data.password = ''
        connection_data.ashost = '52.166.148.139'
        connection_data.sysnr = 0
        connection_data.client = 100
        cut = CitAbapClient.connect_to(connection_data)
        count = cut.get_rowcount('WF_LS')
        self.assertEqual(100, count)

    def test_count_records(self):
        cut = CitAbapClient.connect_to(get_connection_data())
        count = cut.get_rowcount('WS_LS')
        self.assertEqual(100, count)

    def test_update_records(self):
        cut = CitAbapClient.connect_to(get_connection_data())
        old_count = cut.get_rowcount('WN_LM')
        updated_record_ids = cut.update_records('WN_LM', 10)
        new_count = cut.get_rowcount('WN_LM')
        self.assertEqual(10, len(updated_record_ids))
        self.assertEqual(old_count, new_count)

    def test_delete_records(self):
        cut = CitAbapClient.connect_to(get_connection_data())
        old_count = cut.get_rowcount('WN_LM')
        deleted_record_ids = cut.delete_records('WN_LM', 10)
        new_count = cut.get_rowcount('WN_LM')
        self.assertEqual(10, len(deleted_record_ids))
        self.assertEqual(10, old_count - new_count)

    def test_insert_records(self):
        cut = CitAbapClient.connect_to(get_connection_data())
        old_count = cut.get_rowcount('WN_LM')
        inserted_record_ids = cut.insert_records('WN_LM', 10)
        new_count = cut.get_rowcount('WN_LM')
        self.assertEqual(10, len(inserted_record_ids))
        self.assertEqual(10, new_count - old_count)

    def test_setup_table(self):
        cut = CitAbapClient.connect_to(get_connection_data())
        dataset_id = 'WS_LS'
        count = cut.setup_table(dataset_id)
        self.assertEqual(100, count)

    def test_teardown_table(self):
        cut = CitAbapClient.connect_to(get_connection_data())
        is_cleared = cut.teardown_table('WS_LS')
        self.assertTrue(is_cleared)

    def test_connection_to_abap_with_lt_prefix(self):
        # Make sure that you provide the credentials either in DMIS_2018.secrets.json or
        # have them in environment variables:
        # DMIS_2018_USER =
        # DMIS_2018_PASSWORD =
        test_path = os.path.join(os.path.dirname(
            __file__), 'testdata', 'connectiondata')

        connection_data = ConnectionData.for_abap('DMIS_2018', test_path)
        cut = CitAbapClient.connect_to(connection_data)
        count = cut.get_rowcount('WN_LS')
        self.assertEqual(100, count)
