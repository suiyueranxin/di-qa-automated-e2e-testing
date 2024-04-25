import os
from time import sleep
import unittest
import uuid
from framework.infrastructure.Cluster import Cluster
from framework.infrastructure.Rms import ChangerequeststatusStatus, Replicationflow
from framework.infrastructure.Modeler import ModelerReplication
from framework.infrastructure.Utils import ConnectionData
from framework.infrastructure.replications.Replication import ReplicationLoadtype, ReplicationSpaceFileDelimiter, ReplicationSpaceFileType, ReplicationSpaceGroupDeltaBy

def get_connection_data():
    # Make sure that you provide the credentials either in CIT.secrets.json or
    # have them in environment variables:
    # CIT_USER =
    # CIT_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'testdata', 'connectiondata')

    connection_data = ConnectionData.for_cluster('CIT', test_path)
    return connection_data

def create_replication_with_task(cluster: Cluster) -> ModelerReplication:
    replicationname = "repli-" + str(uuid.uuid4())
    path = 'files/rms/' + replicationname + '.replication'
    replication = cluster.modeler.replications.create_replication(
            replicationname)
    replication.set_description('This is the description')
    replication.set_sourcespace('S4H_2020', '/CDS')
    replication.set_targetspace('HANA_CLOUD', '/CIT_TEST')
    task = replication.create_task('SEPM_I_SALESORDERTEXT_E')
    task.set_targetdataset(
        'CIT_ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER')
    task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)
    replication.save()
    return replication


def connect_to_cluster() -> Cluster:
    connectionData = get_connection_data()
    cluster = Cluster.connect_to(connectionData)
    return cluster


class testReplicationBasics(unittest.TestCase):
    def test_basics(self):
        connectionData = get_connection_data()
        cluster = Cluster.connect_to(connectionData)
        self.assertIsNotNone(cluster, 'Connection to cluster failed!')

        replicationname = 'E2Etest123'

        repository = cluster.repository
        path = 'files/rms/' + replicationname + '.replication'
        if repository.exists('user', path):
            status = repository.remove('user', path)
            self.assertIsNotNone(status)
            self.assertEqual(200, status.status_code)

        replication = cluster.modeler.replications.create_replication(
            replicationname)

        self.assertTrue(repository.exists('user', path))

        replication.set_description('This is the description')

        replication.set_sourcespace('S4H_2021', '/CDS')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')

        task = replication.create_task('Z_SEPM_I_SALESORDER')
        task.set_targetdataset(
            'CIT_ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER')

        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(replicationflow)

        status = replicationflow.getchangerequeststatus()

        # replication.run()

        #repository.remove('user', path)

    def test_CDS_basics(self):
        connectionData = get_connection_data()
        cluster = Cluster.connect_to(connectionData)
        self.assertIsNotNone(cluster, 'Connection to cluster failed!')

        replicationname = 'E2E_basicCDS_123'
        replication = cluster.modeler.replications.create_replication(
            replicationname)

        replication.set_description('Created via testing framework')

        replication.set_sourcespace('S4H_2021_QOI', '/CDS')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        replication.create_task('ZTS_COUNTRY')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replicationname} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    def test_ObjectStoreS3_basics(self):
        connectionData = get_connection_data()
        cluster = Cluster.connect_to(connectionData)
        self.assertIsNotNone(cluster, 'Connection to cluster failed!')

        replication = cluster.modeler.replications.create_replication(
            'E2E_basicObjectStore_123')

        replication.set_description('Created via testing framework')

        replication.set_sourcespace('S4H_2021_QOI', '/CDS')
        targetspace = replication.set_targetspace('S3_CIT', '/D050579/E2E')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.NONE)
        targetspace.set_file_type(ReplicationSpaceFileType.CSV)
        # targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        targetspace.set_file_delimiter(ReplicationSpaceFileDelimiter.COMMA)
        targetspace.set_file_header(True)

        replication.create_task('ZTS_COUNTRY')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    def test_ObjectStoreADL_basics(self):
        connectionData = get_connection_data()
        cluster = Cluster.connect_to(connectionData)
        self.assertIsNotNone(cluster, 'Connection to cluster failed!')

        replication = cluster.modeler.replications.create_replication(
            'E2E_basicObjectStore_006')

        replication.set_description('Created via testing framework')

        replication.set_sourcespace('S4H_2021_QOI', '/CDS')
        targetspace = replication.set_targetspace('ADLv2', '/D050579/E2E')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.NONE)
        targetspace.set_file_type(ReplicationSpaceFileType.CSV)
        # targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        targetspace.set_file_delimiter(ReplicationSpaceFileDelimiter.COMMA)
        targetspace.set_file_header(True)

        replication.create_task('ZTS_COUNTRY')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    def test_deploy_replication_from_repository(self):
        connectionData = get_connection_data()
        cluster = Cluster.connect_to(connectionData)
        self.assertIsNotNone(cluster, 'Connection to cluster failed!')

        replication = cluster.modeler.replications.open_replication(
            'abap-cds-to-hc-small')

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    def test_undeploy_replication_from_repository(self):
        cluster = connect_to_cluster()
        replication = create_replication_with_task(cluster)
        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')
        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                        changerequeststatus.getstatus(), changerequeststatus)
        replicationflow = replication.undeploy()
        self.assertIsNotNone(
            replicationflow, f'Undeployment of replication {replication.name} failed!')
        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                        changerequeststatus.getstatus(), changerequeststatus)
        cluster.modeler.replications.delete_replication(replication.name)
    
    def test_delete_replication_from_repository(self):
        cluster = connect_to_cluster()
        replication = create_replication_with_task(cluster)
        replicationname = replication.name
        cluster.modeler.replications.delete_replication(replicationname)
        path = 'files/rms/' + replicationname + '.replication'
        repository = cluster.repository
        self.assertFalse(repository.exists('user',path))
            



if __name__ == '__main__':
    unittest.main()
