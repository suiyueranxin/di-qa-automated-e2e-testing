import unittest

from framework.infrastructure.connections.Connection import Connection


class testConnection(unittest.TestCase):

    def test_basics(self):
        cut = Connection('S4H_2021')
        self.assertEqual('S4H_2021', cut.id)
        self.assertEqual('S4H_2021', cut._values['id'])

    def test_description(self):
        cut = Connection('S4H_2021')
        self.assertEqual('', cut._values['description'])
        cut.description = 'just a test'
        self.assertEqual('just a test', cut.description)
        self.assertEqual('just a test', cut._values['description'])

    def test_type(self):
        cut = Connection('SomeConnectionId')
        self.assertEqual(None, cut.type)
        cut.type = 'ABAP'
        self.assertEqual('ABAP', cut.type)

    def test_ccmtypeid(self):
        cut = Connection('SomeConnectionId')
        self.assertEqual(None, cut.type)
        cut.ccmTypeId = 'ABAP'
        self.assertEqual('ABAP', cut.ccmTypeId)

    def test_setvalues(self):
        values = {}
        values['id'] = 'idFromValues'
        cut = Connection('SomeConnectionId')
        cut._values = values
        self.assertEqual('idFromValues', cut.id)


if __name__ == '__main__':
    unittest.main()
