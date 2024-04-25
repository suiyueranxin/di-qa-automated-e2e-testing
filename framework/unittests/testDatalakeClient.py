
import unittest

from framework.validation.datalake.DatalakeClient import DatalakeConnectionData


class testDatalakeConnectionData(unittest.TestCase):

    def test_basics(self):
        cut = DatalakeConnectionData('DataLake')
        self.assertEqual('DataLake', cut.name)

    def test_properties(self):
        cut = DatalakeConnectionData('DataLake')
        cut.accountname = 'dataintegration2021'
        self.assertEqual('dataintegration2021', cut.accountname)
        cut.container = 'dataintegration2021'
        self.assertEqual('dataintegration2021', cut.container)
        cut.accountkey = "123456"
        self.assertEqual('123456', cut.accountkey)
