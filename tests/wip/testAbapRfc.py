import os
import unittest
from unittest.case import skip
from framework.infrastructure.Utils import ConnectionData


from framework.validation.abap.AbapClient import AbapClient


class testAbapRfc(unittest.TestCase):

    def create_client(self) -> AbapClient:
        test_path = os.path.join(os.path.dirname(
            __file__), '..', 'CIT', 'connectiondata')

        connection_data = ConnectionData.for_abap('SAL', test_path)
        abapclient = AbapClient.connect_to(connection_data)
        return abapclient

    def test_get_rowcount(self):
        abapclient = self.create_client()

        sourcecount = abapclient.get_rowcount('WS_LS')
        print(sourcecount)
        self.assertEqual(100, sourcecount)

    def test_get_tablename(self):
        abapclient = self.create_client()

        tablename = abapclient.get_tablename('WS_LS')
        print(tablename)
        self.assertEqual('DHE2E_WS_LS', tablename)

    def test_get_cdsname(self):
        abapclient = self.create_client()

        cdsname = abapclient.get_cdsname('WS_LS')
        print(cdsname)
        self.assertEqual('DHE2E_CDS_WS_LS', cdsname)

    def test_update_records(self):
        abapclient = self.create_client()

        record_ids = abapclient.update_records('WS_LS', 5)
        print(record_ids)
        self.assertEqual(5, len(record_ids))
