import unittest

from framework.unittests.doubles.AbapMock import AbapConnectionMock
from framework.validation.abap.CitAbapClient import CitAbapClient


class testCitAbapClient(unittest.TestCase):

    def test_get_rowcount(self):
        cut = CitAbapClient()
        abapmock = AbapConnectionMock()
        cut._connection = abapmock

        rfc_result = {}
        rfc_result['EV_RC'] = 0
        rfc_result['EV_COUNT'] = '0000000234'
        rfc_result['EV_TABNAME'] = 'DHE2E_WS_LS'
        rfc_result['EV_CDSNAME'] = 'DHE2E_CDS_WS_LS'
        rfc_result['ET_RECID'] = []

        rfc_result['EV_JOBNUM'] = ''
        rfc_result['EV_JSON'] = '{"rc":0,"message":"OK","count":"234","cdsname":"DHE2E_CDS_WS_LS","tabname":"DHE2E_WS_LS","timestamp":"0.0000000","recid":[]}'
        rfc_result['EV_MESSAGE'] = 'OK'
        rfc_result['EV_STATUS'] = 'FINISHED'

        abapmock.set_rfc_result(rfc_result)

        count = cut.get_rowcount('WS_LS')
        self.assertEqual(234, count)

        rfc_result['EV_COUNT'] = 1304
        count = cut.get_rowcount('WS_LS')
        self.assertEqual(1304, count)
        expectedcall = ['DHE2E_CIT_RFC_DATA_ACCESS',
                        {'IV_TABLE_ID': 'WS_LS', 'IV_MODE': 'C', 'IV_NUM_RECS': '0'}]
        self.assertEqual(expectedcall, abapmock.last_rfc_call)

        tablename = cut.get_tablename('WS_LS')
        self.assertEqual('DHE2E_WS_LS', tablename)

        cdsname = cut.get_cdsname('WS_LS')
        self.assertEqual('DHE2E_CDS_WS_LS', cdsname)

        rfc_result['EV_RC'] = 1
        rfc_result['EV_MESSAGE'] = 'Test for errors'
        self.assertRaises(RuntimeError, cut.get_rowcount, 'WS_LS')
        self.assertRaises(RuntimeError, cut.get_tablename, 'WS_LS')
        self.assertRaises(RuntimeError, cut.get_cdsname, 'WS_LS')

    def test_update_records(self):
        cut = CitAbapClient()
        abapmock = AbapConnectionMock()
        cut._connection = abapmock

        rfc_result = {}
        rfc_result['EV_RC'] = 0
        rfc_result['EV_COUNT'] = '0000000003'
        rfc_result['EV_TABNAME'] = 'DHE2E_WN_LM'
        rfc_result['EV_CDSNAME'] = 'DHE2E_CDS_WN_LM'
        rfc_result['ET_RECID'] = ['001', '007', '123']

        rfc_result['EV_JOBNUM'] = ''
        rfc_result['EV_JSON'] = '{"rc":0,"message":"OK","count":"3","cdsname":"DHE2E_CDS_WN_LM","tabname":"DHE2E_WN_LM","timestamp":"20220223192633.8732140","recid":["001", "007", "123"]}'
        rfc_result['EV_MESSAGE'] = 'OK'
        rfc_result['EV_STATUS'] = 'FINISHED'

        abapmock.set_rfc_result(rfc_result)

        record_ids = cut.update_records('WN_LM', 3)
        self.assertEqual(3, len(record_ids))
        self.assertEqual(['001', '007', '123'], record_ids)

        expectedcall = ['DHE2E_CIT_RFC_DATA_ACCESS',
                        {'IV_TABLE_ID': 'WN_LM', 'IV_MODE': 'U', 'IV_NUM_RECS': '3'}]
        self.assertEqual(expectedcall, abapmock.last_rfc_call)

        rfc_result['EV_RC'] = 1
        rfc_result['EV_MESSAGE'] = 'Test for errors'
        self.assertRaises(RuntimeError, cut.update_records, 'WS_LS', 3)

    def test_delete_records(self):
        cut = CitAbapClient()
        abapmock = AbapConnectionMock()
        cut._connection = abapmock

        rfc_result = {}
        rfc_result['EV_RC'] = 0
        rfc_result['EV_COUNT'] = '0000000003'
        rfc_result['EV_TABNAME'] = 'DHE2E_WN_LM'
        rfc_result['EV_CDSNAME'] = 'DHE2E_CDS_WN_LM'
        rfc_result['ET_RECID'] = ['001', '007', '123']

        rfc_result['EV_JOBNUM'] = ''
        rfc_result['EV_JSON'] = '{"rc":0,"message":"OK","count":"3","cdsname":"DHE2E_CDS_WN_LM","tabname":"DHE2E_WN_LM","timestamp":"20220223192633.8732140","recid":["001", "007", "123"]}'
        rfc_result['EV_MESSAGE'] = 'OK'
        rfc_result['EV_STATUS'] = 'FINISHED'

        abapmock.set_rfc_result(rfc_result)

        record_ids = cut.delete_records('WN_LM', 3)
        self.assertEqual(3, len(record_ids))
        self.assertEqual(['001', '007', '123'], record_ids)

        expectedcall = ['DHE2E_CIT_RFC_DATA_ACCESS',
                        {'IV_TABLE_ID': 'WN_LM', 'IV_MODE': 'D', 'IV_NUM_RECS': '3'}]
        self.assertEqual(expectedcall, abapmock.last_rfc_call)

    def test_insert_records(self):
        cut = CitAbapClient()
        abapmock = AbapConnectionMock()
        cut._connection = abapmock

        rfc_result = {}
        rfc_result['EV_RC'] = 0
        rfc_result['EV_COUNT'] = '0000000003'
        rfc_result['EV_TABNAME'] = 'DHE2E_WN_LM'
        rfc_result['EV_CDSNAME'] = 'DHE2E_CDS_WN_LM'
        rfc_result['ET_RECID'] = ['001', '007', '123']

        rfc_result['EV_JOBNUM'] = ''
        rfc_result['EV_JSON'] = '{"rc":0,"message":"OK","count":"3","cdsname":"DHE2E_CDS_WN_LM","tabname":"DHE2E_WN_LM","timestamp":"20220223192633.8732140","recid":["001", "007", "123"]}'
        rfc_result['EV_MESSAGE'] = 'OK'
        rfc_result['EV_STATUS'] = 'FINISHED'

        abapmock.set_rfc_result(rfc_result)

        record_ids = cut.insert_records('WN_LM', 3)
        self.assertEqual(3, len(record_ids))
        self.assertEqual(['001', '007', '123'], record_ids)

        expectedcall = ['DHE2E_CIT_RFC_DATA_ACCESS',
                        {'IV_TABLE_ID': 'WN_LM', 'IV_MODE': 'I', 'IV_NUM_RECS': '3'}]
        self.assertEqual(expectedcall, abapmock.last_rfc_call)

    def test_cit_prefix(self):
        cut = CitAbapClient()
        abapmock = AbapConnectionMock()
        cut._connection = abapmock

        cut._cit_prefix = 'FOOBAR'

        rfc_result = {}
        rfc_result['EV_RC'] = 0
        rfc_result['EV_COUNT'] = 234
        rfc_result['EV_TABNAME'] = 'DHE2E_WS_LS'
        rfc_result['EV_CDSNAME'] = 'DHE2E_CDS_WS_LS'
        abapmock.set_rfc_result(rfc_result)

        count = cut.get_rowcount('WS_LS')
        self.assertEqual(234, count)

        rfc_result['EV_COUNT'] = 1304
        count = cut.get_rowcount('WS_LS')
        self.assertEqual(1304, count)
        expectedcall = ['FOOBAR_CIT_RFC_DATA_ACCESS',
                        {'IV_TABLE_ID': 'WS_LS', 'IV_MODE': 'C'}]
