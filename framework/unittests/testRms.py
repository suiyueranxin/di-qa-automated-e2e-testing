import unittest

from framework.infrastructure.Cluster import Cluster, ClusterConnectionData
from framework.infrastructure.Modeler import ModelerReplicationJson
from framework.infrastructure.Rms import Replicationflow, ChangerequeststatusStatus, Rms
from framework.infrastructure.replications.Replication import Replication
from framework.unittests.doubles.SessionMock import SessionMock


def getDummyConnectionData():
    connectionData = ClusterConnectionData('POD-INT', 'https://cluster')
    connectionData.tenant = 'default'
    connectionData.user = 'tester'
    connectionData.password = '********'
    return connectionData


class testRms(unittest.TestCase):

    def test_skeleton(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        cut = Rms(cluster)

    def test_createreplicationflow(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        replication = Replication('ABAP_CDS_S4H_to_HC_deltaSkinny')
        replication.set_description('test')
        replication._version = "ONE_SOURCE_ONE_TARGET"

        replication.set_sourcespace('S4H_2021', '/CDS')
        replication.sourcespace._connectiontype = 'ABAP'
        replication.sourcespace._ccmconnectiontype = 'ABAP'

        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        replication.targetspace._connectiontype = 'HANA_DB'
        replication.targetspace._ccmconnectiontype = 'HANA'

        task = replication.create_task('Z_SEPM_I_SALESORDER')
        task.set_targetdataset(
            'CIT_ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER')

        # only for testing! should not be done in 'real' code!
        task._values['name'] = 'ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER_4ztxt9'

        cut = Rms(cluster)

        sessionmock.setresponsecontent(
            202, '{"url": "/api/dt/v1/replicationflows/E2Etest008/changerequeststatus"}')
        replicationflow = cut.createreplicationflow(replication)
        self.assertTrue(
            '/app/rms/api/dt/v1/replicationflows' in sessionmock.lastcalledurl)

        jsonContent = sessionmock.posteddata
        verficationContent = ModelerReplicationJson.serialize(replication)

        self.assertEquals(verficationContent, jsonContent)

        self.assertIsNotNone(replicationflow)
        self.assertEqual('ABAP_CDS_S4H_to_HC_deltaSkinny',
                         replicationflow._name)

        sessionmock.setresponsecontent(404, '')
        replicationflow = cut.createreplicationflow(replication)
        self.assertIsNone(replicationflow)

    def test_deletereplicationflow(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock
        replication = Replication('ABAP_CDS_S4H_to_HC_deltaSkinny')
        cut = Rms(cluster)
        sessionmock.setresponsecontent(
            202, '{"url": "/api/dt/v1/replicationflows/ABAP_CDS_S4H_to_HC_deltaSkinny/changerequeststatus"}')
        replicationflow = cut.deletereplicationflow(replication)
        self.assertIsNotNone(replicationflow)
        self.assertEqual('ABAP_CDS_S4H_to_HC_deltaSkinny',
                         replicationflow._name)
        self.assertTrue(
            '/app/rms/api/dt/v1/replicationflows' in sessionmock.lastcalledurl)
        sessionmock.setresponse(404)
        replicationflow = cut.deletereplicationflow(replication)
        self.assertIsNone(replicationflow)


class testReplicationflow(unittest.TestCase):

    def test_skeleton(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        rms = Rms(cluster)

        cut = Replicationflow('Just_a_name', rms)
        self.assertEqual('Just_a_name', cut._name)
        self.assertEqual(rms, cut._rms)

    def test_getchangerequeststatus(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        rms = Rms(cluster)

        cut = Replicationflow('E2Etest008', rms)
        changerequeststatus = cut.getchangerequeststatus()
        self.assertIsNone(changerequeststatus)

        # only for testing! should not be done in 'real' code!
        cut._changerequeststatusurl = '/api/dt/v1/replicationflows/E2Etest008/changerequeststatus'

        sessionmock.setresponsecontent(
            200, '{"requestType":"ACTIVATE_REPLICATION_FLOW","status":"ERROR","requestedAt":"2021-11-24T07:53:07.7084525Z","requestCompletedAt":"2021-11-24T07:53:09.2900162Z","newObjects":[{"name":"E2ESLTtest001_S4H_2021_src","type":"SOURCE_SPACE","status":"VALIDATED"},{"name":"E2ESLTtest001_HANA_CLOUD_tgt","type":"TARGET_SPACE","status":"VALIDATED"},{"name":"E2ESLTtest001","type":"CONSTELLATION","status":"VALIDATED"},{"name":"SNWD_SO","type":"SOURCE_DATASET","status":"ERROR","error":"table already in use by replication flow ABAP_SLT_S4H_to_HC_deltaSkinny"},{"name":"CIT_ABAP_SLT_S4H_to_HC_deltaSkinny_SNWD_SO","type":"TARGET_DATASET","status":"ERROR","error":"table already in use by replication flow ABAP_SLT_S4H_to_HC_deltaSkinny"},{"name":"E2ESLTtest001_SNWD_SO_fvpjjq","type":"TASK","status":"VALIDATED"}]}')
        changerequeststatus = cut.getchangerequeststatus()
        self.assertIsNotNone(changerequeststatus)
        self.assertTrue(
            '/app/rms/api/dt/v1/replicationflows/E2Etest008/changerequeststatus' in sessionmock.lastcalledurl)
        self.assertEqual(ChangerequeststatusStatus.ERROR,
                         changerequeststatus.getstatus())

    def test_runorresume(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        rms = Rms(cluster)

        cut = Replicationflow('E2Etest008', rms)
        changerequeststatus = cut.getchangerequeststatus()
        self.assertIsNone(changerequeststatus)

        # only for testing! should not be done in 'real' code!
        cut._changerequeststatusurl = '/api/dt/v1/replicationflows/E2Etest008/changerequeststatus'

        sessionmock.setresponsecontent(
            202, '{"url": "/api/dt/v1/replicationflows/E2Etest008/changerequeststatus"}')
        cut.runorresume()
        self.assertTrue(
            '/app/rms/api/dt/v1/replicationflows/E2Etest008?requestType=RUN_OR_RESUME_ALL_INACTIVE_TASKS' in sessionmock.lastcalledurl)

    def test_waitwhile(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        rms = Rms(cluster)

        cut = Replicationflow('E2Etest008', rms)

        # only for testing! should not be done in 'real' code!
        cut._changerequeststatusurl = '/api/dt/v1/replicationflows/E2Etest008/changerequeststatus'
        cut._waittime = 0

        sessionmock.setresponsecontent(200, '{"status": "PROCESSING"}')
        sessionmock.setresponsecontent(200, '{"status": "PROCESSING"}')
        sessionmock.setresponsecontent(200, '{"status": "VALIDATING"}')
        sessionmock.setresponsecontent(200, '{"status": "COMPLETED"}')
        changerequeststatus = cut.waitwhile(
            [ChangerequeststatusStatus.PROCESSING])
        self.assertEqual(ChangerequeststatusStatus.VALIDATING,
                         changerequeststatus.getstatus())

        sessionmock.setresponsecontent(200, '{"status": "PROCESSING"}')
        sessionmock.setresponsecontent(200, '{"status": "ERROR"}')
        sessionmock.setresponsecontent(200, '{"status": "PROCESSING"}')
        sessionmock.setresponsecontent(200, '{"status": "VALIDATING"}')
        sessionmock.setresponsecontent(200, '{"status": "COMPLETED"}')
        changerequeststatus = cut.waitwhile(
            [ChangerequeststatusStatus.PROCESSING, ChangerequeststatusStatus.COMPLETED, ChangerequeststatusStatus.ERROR])
        self.assertEqual(ChangerequeststatusStatus.VALIDATING,
                         changerequeststatus.getstatus())

    def test_waitwhilebusy(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        rms = Rms(cluster)

        cut = Replicationflow('E2Etest008', rms)

        # only for testing! should not be done in 'real' code!
        cut._changerequeststatusurl = '/api/dt/v1/replicationflows/E2Etest008/changerequeststatus'
        cut._waittime = 0

        sessionmock.setresponsecontent(200, '{"status": "PROCESSING"}')
        sessionmock.setresponsecontent(200, '{"status": "VALIDATING"}')
        sessionmock.setresponsecontent(200, '{"status": "COMPLETED"}')
        changerequeststatus = cut.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus())

        sessionmock.setresponsecontent(200, '{"status": "VALIDATING"}')
        sessionmock.setresponsecontent(200, '{"status": "PROCESSING"}')
        sessionmock.setresponsecontent(200, '{"status": "ERROR"}')
        sessionmock.setresponsecontent(200, '{"status": "COMPLETED"}')
        changerequeststatus = cut.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.ERROR,
                         changerequeststatus.getstatus())


class testChangerequeststatus(unittest.TestCase):

    def test_basics(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        rms = Rms(cluster)

        replicationflow = Replicationflow('E2Etest008', rms)

        replicationflow._changerequeststatusurl = '/api/dt/v1/replicationflows/E2Etest008/changerequeststatus'
        sessionmock.setresponsecontent(
            200, '{"requestType":"ACTIVATE_REPLICATION_FLOW","status":"ERROR","requestedAt":"2021-11-24T07:53:07.7084525Z","requestCompletedAt":"2021-11-24T07:53:09.2900162Z","newObjects":[{"name":"E2ESLTtest001_S4H_2021_src","type":"SOURCE_SPACE","status":"VALIDATED"},{"name":"E2ESLTtest001_HANA_CLOUD_tgt","type":"TARGET_SPACE","status":"VALIDATED"},{"name":"E2ESLTtest001","type":"CONSTELLATION","status":"VALIDATED"},{"name":"SNWD_SO","type":"SOURCE_DATASET","status":"ERROR","error":"table already in use by replication flow ABAP_SLT_S4H_to_HC_deltaSkinny"},{"name":"CIT_ABAP_SLT_S4H_to_HC_deltaSkinny_SNWD_SO","type":"TARGET_DATASET","status":"ERROR","error":"table already in use by replication flow ABAP_SLT_S4H_to_HC_deltaSkinny"},{"name":"E2ESLTtest001_SNWD_SO_fvpjjq","type":"TASK","status":"VALIDATED"}]}')

        cut = replicationflow.getchangerequeststatus()
        self.assertEqual(ChangerequeststatusStatus.ERROR, cut.getstatus())


class testChangerequeststatusStatus(unittest.TestCase):
    def test_enumvalues(self):
        cut = ChangerequeststatusStatus.PROCESSING
        cut = ChangerequeststatusStatus.VALIDATING
        cut = ChangerequeststatusStatus.COMPLETED
        cut = ChangerequeststatusStatus.ERROR
        cut = ChangerequeststatusStatus.PENDING
