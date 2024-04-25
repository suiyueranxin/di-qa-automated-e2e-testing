import os
from pathlib import Path
import unittest

from framework.infrastructure.Utils import Utils


class testUtils(unittest.TestCase):

    def test_rootfolder(self):
        Utils.setrootfolder(Path.cwd())
        self.assertEqual(Path.cwd(), Utils.getrootfolder())
        Utils.setrootfolder('c:\\')
        self.assertEqual(Path('c:\\'), Utils.getrootfolder())

    def test_readjson(self):
        Utils.setrootfolder(Path.cwd() / 'framework/integrationtests')
        data = Utils.readjson('testdata/sampleReplication.json')
        self.assertEqual('ABAP_CDS_S4H_to_HC_deltaSkinny', data['name'])

    def test_writejson(self):
        Utils.setrootfolder(Path.cwd() / 'framework/integrationtests/testdata')
        if Path.exists(Utils.getrootfolder() / 'dummy.json'):
            os.remove(Utils.getrootfolder() / 'dummy.json')
        data = {}
        data['foo'] = 'bar'
        Utils.writejson('dummy.json', data)
        dataverification = Utils.readjson('dummy.json')
        self.assertEqual('bar', dataverification['foo'])

    def test_deletefile(self):
        Utils.setrootfolder(Path.cwd() / 'framework/integrationtests/testdata')
        data = {}
        data['foo'] = 'bar'
        Utils.writejson('dummy.json', data)
        self.assertTrue(Path.exists(Utils.getrootfolder() / 'dummy.json'))
        Utils.deletefile('dummy.json')
        self.assertFalse(Path.exists(Utils.getrootfolder() / 'dummy.json'))
  

if __name__ == '__main__':
    unittest.main()
