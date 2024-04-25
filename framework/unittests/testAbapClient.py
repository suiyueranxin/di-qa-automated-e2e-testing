import unittest

from framework.unittests.doubles.AbapMock import AbapConnectionMock
from framework.validation.abap.AbapClient import AbapClient, AbapConnectionData


class testAbapClient(unittest.TestCase):

    def test_basics(self):
        cut = AbapClient()
        abapmock = AbapConnectionMock()
        cut._connection = abapmock

        rfc_result = {}
        rfc_result['EV_COUNT'] = '0000000234'

        abapmock.set_rfc_result(rfc_result)

        rfc_result = cut.call_rfc(
            'DHE2E_CIT_RFC_DATA_ACCESS', IV_TABLE_ID='WS_LS', IV_MODE='C', IV_NUM_RECS='0')
        count = int(rfc_result['EV_COUNT'])
        self.assertEqual(234, count)

        rfc_result['EV_COUNT'] = 1304
        rfc_result = cut.call_rfc(
            'DHE2E_CIT_RFC_DATA_ACCESS', IV_TABLE_ID='WS_LS', IV_MODE='C', IV_NUM_RECS='0')
        count = int(rfc_result['EV_COUNT'])
        self.assertEqual(1304, count)
        expectedcall = ['DHE2E_CIT_RFC_DATA_ACCESS',
                        {'IV_TABLE_ID': 'WS_LS', 'IV_MODE': 'C', 'IV_NUM_RECS': '0'}]
        self.assertEqual(expectedcall, abapmock.last_rfc_call)


class testAbapConnectionData(unittest.TestCase):

    def test_basics(self):
        cut = AbapConnectionData('S4H')
        self.assertEqual('S4H', cut.name)

    def test_properties(self):
        cut = AbapConnectionData('S4H')
        cut.user = 'someUserName'
        self.assertEqual('someUserName', cut.user)
        cut.password = 'secret*Password'
        self.assertEqual('secret*Password', cut.password)
        cut.ashost = 'app.host.sap.com'
        self.assertEqual('app.host.sap.com', cut.ashost)
        cut.sysnr = 5
        self.assertEqual('05', cut.sysnr)
        cut.sysnr = '23'
        self.assertEqual('23', cut.sysnr)
        cut.client = 7
        self.assertEqual('007', cut.client)
        cut.client = '13'
        self.assertEqual('013', cut.client)
        cut.mshost = 'msgsrv.host.sap.com'
        self.assertEqual('msgsrv.host.sap.com', cut.mshost)
        cut.sysid = 'FOO'
        self.assertEqual('FOO', cut.sysid)
        cut.group = 'PUBLIC'
        self.assertEqual('PUBLIC', cut.group)
