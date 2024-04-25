import unittest

from framework.infrastructure.Cluster import Cluster, ClusterConnectionData
from framework.infrastructure.Repository import Repositoy
from framework.unittests.doubles.SessionMock import SessionMock


def getDummyConnectionData():
    connectionData = ClusterConnectionData('POD-INT', 'https://cluster')
    connectionData.tenant = 'default'
    connectionData.user = 'tester'
    connectionData.password = '********'
    return connectionData


class testRepository(unittest.TestCase):

    def test_skeleton(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        cut = Repositoy(cluster)

        sessionmock.setresponse(200)
        sessionmock.setresponse(200)
        sessionmock.setresponse(200)
        sessionmock.setresponse(200)
        response = cut.get_status('spacetype', 'path')
        response = cut.write('spacetype', 'path', 'filecontent')
        response = cut.remove('spacetype', 'path')
        response = cut.read('spacetype', 'path')

    def test_existst(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        cut = Repositoy(cluster)

        sessionmock.setresponse(200)
        exists = cut.exists('spacetype', 'path')
        self.assertTrue(exists)

        sessionmock.setresponse(404)
        exists = cut.exists('spacetype', 'path')
        self.assertFalse(exists)


if __name__ == '__main__':
    unittest.main()
