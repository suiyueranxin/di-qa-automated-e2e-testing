import os
import unittest

from framework.infrastructure.Utils import ConnectionData
from framework.validation.abap.CitAbapClient import CitAbapClient

SYSTEM_ID = "SAL"


class testCIT_Content(unittest.TestCase):
    """Performs a check of all CIT tables for expected table sizes.

    The expected table sizes are taken from:
    https://wiki.wdf.sap.corp/wiki/display/Odin/Test+Data+for+CIT
    """

    abap_system = None

    @classmethod
    def setUp(self) -> None:
        if self.abap_system == None:
            # Make sure that you provide the credentials either in SAL.secrets.json or
            # have them in environment variables:
            # SAL_USER =
            # SAL_PASSWORD =
            test_path = os.path.join(os.path.dirname(
                __file__), 'connectiondata')

            connection_data = ConnectionData.for_abap(SYSTEM_ID, test_path)
            self.abap_system = CitAbapClient.connect_to(connection_data)

            self.assertIsNotNone(
                self.abap_system, 'Connection to cluster failed!')

    def _check_size(self, table_id, expected_size):
        count = self.abap_system.get_rowcount(table_id)
        self.assertEqual(expected_size, count)

    def test_size_WS_LS(self):
        self._check_size('WA_LS',	100)

    def test_size_WF_LL(self):
        self._check_size('WF_LL',	10000000)

    def test_size_WF_LM(self):
        self._check_size('WF_LM',	1000000)

    def test_size_WF_LS(self):
        self._check_size('WF_LS',	100)

    def test_size_WF_LX(self):
        self._check_size('WF_LX',	100000000)

    def test_size_WN_LL(self):
        self._check_size('WN_LL',	10000000)

    def test_size_WN_LM(self):
        self._check_size('WN_LM',	1000000)

    def test_size_WN_LS(self):
        self._check_size('WN_LS',	100)

    def test_size_WN_LX(self):
        self._check_size('WN_LX',	100000000)

    def test_size_WS_LL(self):
        self._check_size('WS_LL',	10000000)

    def test_size_WS_LM(self):
        self._check_size('WS_LM',	1000000)

    def test_size_WS_LS(self):
        self._check_size('WS_LS',	100)

    def test_size_WS_LX(self):
        self._check_size('WS_LX',	100000000)

    def test_size_WX_LL(self):
        self._check_size('WX_LL',	10000000)

    def test_size_WX_LM(self):
        self._check_size('WX_LM',	1000000)

    def test_size_WX_LS(self):
        self._check_size('WX_LS',	100)

    def test_size_WX_LX(self):
        self._check_size('WX_LX',	100000000)
