import unittest

from framework.infrastructure.Cluster import Cluster, ClusterHeaders, ClusterUrls, ClusterConnectionData
from framework.infrastructure.Repository import Repositoy
from framework.unittests.doubles.SessionMock import SessionMock


def getDummyConnectionData():
    connectionData = ClusterConnectionData('POD-INT', 'https://cluster')
    connectionData.tenant = 'default'
    connectionData.user = 'tester'
    connectionData.password = '********'
    return connectionData


class testCluster(unittest.TestCase):

    # This is where the basic setup logic needs to be tested.
    def test_basics(self):
        cut = Cluster(getDummyConnectionData())

    def test_Modeler(self):
        cut = Cluster(getDummyConnectionData())
        modeler = cut.modeler
        self.assertIsNotNone(modeler)

    def test_Repository(self):
        cut = Cluster(getDummyConnectionData())
        repositoy = cut.repository
        self.assertIsNotNone(repositoy)

    def test_ConnectionManagement(self):
        cut = Cluster(getDummyConnectionData())
        connectionmanagement = cut.connectionmanagement

    def test_Rms(self):
        cut = Cluster(getDummyConnectionData())
        rms = cut.rms

    def test_Monitoring(self):
        cut = Cluster(getDummyConnectionData())
        monitoring = cut.monitoring

    def test_apiget(self):
        cut = Cluster(getDummyConnectionData())
        cut.session = SessionMock()
        cut.session.setresponse(200)
        cut.apiget('path')

    def test_apipost(self):
        cut = Cluster(getDummyConnectionData())
        cut.session = SessionMock()
        cut.session.setresponse(200)
        cut.apipost('path', 'data')

    def test_apidelete(self):
        cut = Cluster(getDummyConnectionData())
        cut.session = SessionMock()
        cut.session.setresponse(200)
        cut.apidelete('path')

    def test_apiput(self):
        cut = Cluster(getDummyConnectionData())
        cut.session = SessionMock()
        cut.session.setresponse(202)
        cut.apiput('path', 'data')


class testClusterConnectionData(unittest.TestCase):

    def test_nameAndBaseurl(self):
        cut = ClusterConnectionData(
            'POD-INT', 'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com')
        self.assertEqual(cut._name, 'POD-INT')
        self.assertEqual(
            cut._baseurl, 'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com')

    def test_tenant(self):
        cut = ClusterConnectionData('foo', 'https://bar')
        cut.tenant = 'default'
        self.assertEqual(cut.tenant, 'default')

    def test_user(self):
        cut = ClusterConnectionData('foo', 'https://bar')
        cut.user = 'admin'
        self.assertEqual(cut.user, 'admin')

    def test_password(self):
        cut = ClusterConnectionData('foo', 'https://bar')
        cut.password = '$ecret!'
        self.assertEqual(cut.password, '$ecret!')

    def test_baseurlChecks(self):
        self.assertRaises(ValueError, ClusterConnectionData, 'foo', 'bar')
        self.assertRaises(ValueError, ClusterConnectionData, 'foo', 'https://')
        self.assertRaises(ValueError, ClusterConnectionData,
                          'foo', 'https:// ')
        self.assertRaises(ValueError, ClusterConnectionData,
                          'foo', 'http://bar.com')
        self.assertRaises(ValueError, ClusterConnectionData,
                          'foo', 'https://abc.com:df/')

    def test_baseurlPathRemoval(self):
        cut = ClusterConnectionData(
            'POD-INT', 'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com/')
        self.assertEqual(
            cut.baseurl, 'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com')
        cut = ClusterConnectionData(
            'POD-INT', 'https://foo/and/some/path?and=query&information#withFragment')
        self.assertEqual(cut.baseurl, 'https://foo')

    def test_late_baseurl_setting(self):
        cut = ClusterConnectionData('WIHTOUT_URL')
        self.assertIsNone(cut.baseurl)


class testClusterUrls(unittest.TestCase):
    def test_basics(self):
        baseurl = 'https://test.k8s-hana.ondemand.com'
        cut = ClusterUrls(baseurl)
        self.assertEqual(baseurl, cut.base)
        self.assertEqual(
            'https://test.k8s-hana.ondemand.com/api/login/v2/finalize', cut.login)


class testClusterHeaders(unittest.TestCase):
    def test_di_headers(self):
        cut = ClusterHeaders()
        self.assertEqual('application/json', cut.di_header['accept'])
        self.assertEqual('application/json', cut.di_header['Content-Type'])
        self.assertEqual('Fetch', cut.di_header['X-Requested-With'])


if __name__ == '__main__':
    unittest.main()
