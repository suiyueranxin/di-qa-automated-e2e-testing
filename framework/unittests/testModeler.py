from asyncio import tasks
import json
import unittest

from requests.sessions import session

from framework.infrastructure.Cluster import Cluster, ClusterConnectionData
from framework.infrastructure.Modeler import Modeler, ModelerReplication, ModelerReplicationJson, ReplicationsModeler
from framework.infrastructure.replications.Replication import Replication, ReplicationLoadtype, ReplicationSpaceFileCompression, ReplicationSpaceFileDelimiter, ReplicationSpaceFileType, ReplicationSpaceGroupDeltaBy, ReplicationSpaceProperty, ReplicationTaskFilter, ReplicationTaskFilterOperator
from framework.unittests.doubles.SessionMock import SessionMock


def getDummyConnectionData():
    connectionData = ClusterConnectionData('POD-INT', 'https://cluster')
    connectionData.tenant = 'default'
    connectionData.user = 'tester'
    connectionData.password = '********'
    return connectionData


class testModeler(unittest.TestCase):

    def test_basics(self):
        cut = Modeler(None)

    def test_Replications(self):
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponse(200)

        cut = Modeler(cluster)
        replications = cut.replications
        replication = replications.create_replication('TestReplicationE2E')
        self.assertEqual('TestReplicationE2E', replication.name)
        self.assertEqual('', replication.description)
        self.assertEqual('ONE_SOURCE_ONE_TARGET', replication.version)


class testReplicationsModeler(unittest.TestCase):

    def test_open_replication(self):
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponsecontent(
            200, get_replication_with_targetspace_properties())

        modeler = Modeler(cluster)
        cut = modeler.replications
        replication = cut.open_replication('cloudFStest')
        self.assertTrue(cluster.session.lastcalledurl.endswith(
            'files/rms/cloudFStest.replication?op=read'))
        self.assertIsNotNone(replication)
        self.assertEqual('A description', replication.description)
        self.assertTrue(replication._modeler == cut)

    def test_delete_replication(self):
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponse(200)
        modeler = Modeler(cluster)
        cut = modeler.replications
        cut.delete_replication('test_replication')
        self.assertTrue(cluster.session.lastcalledurl.endswith(
            'files/rms/test_replication.replication?op=remove'))


class testModelerReplication(unittest.TestCase):

    def test_skeleton(self):
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponse(200)

        modeler = ReplicationsModeler(cluster)

        cut = ModelerReplication('someName', modeler)
        cut.save()
        cut.run()
        cut.suspend()
        cluster.session.setresponsecontent(
            200, '{"url": "/api/dt/v1/replicationflows/test/changerequeststatus"}')
        cut.undeploy()

    def test_setsourcespace(self):
        connectionpayload = """
        {
            "id": "S4H_2021",
            "description": "S4H 2021 - via QOI-927 (GAERTNERNI) ",
            "type": "ABAP",
            "ccmId": "663a485a-b7b6-48df-bc22-34d0527c7961",
            "ccmTypeId": "ABAP",
            "typeServices": [
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "extractMetadata"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "com.sap.dh.connection::statusCheck"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "browse"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "metadata"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "dataPreview"
                }
            ]
        }
        """
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponsecontent(200, connectionpayload)

        modeler = ReplicationsModeler(cluster)

        cut = ModelerReplication('someName', modeler)
        self.assertIsNone(cluster.session.lastcalledurl)
        cut.set_sourcespace('S4H_2021', '/CDS')
        self.assertEqual('S4H_2021', cut.sourcespace._connectionid)
        self.assertEqual('/CDS', cut.sourcespace._container)
        self.assertIsNotNone(cluster.session.lastcalledurl)
        self.assertEqual('ABAP', cut.sourcespace._connectiontype)
        self.assertEqual('ABAP', cut.sourcespace._ccmconnectiontype)

    def test_settargetspace(self):
        connectionpayload = """
        {
            "id": "HANA_CLOUD",
            "description": "CIT HANA CLOUD: 87cc15ab-65ef-4b28-9433-962f7400aecc.hana.canary-eu10.hanacloud.ondemand.com:443",
            "type": "HANA_DB",
            "readOnly": false,
            "ccmId": "44ff8f05-9e9c-4c97-abe2-c0260db87f51",
            "ccmTypeId": "HANA",
            "typeServices": [
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "rulesRemoteSource"
                },
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "extractLineage"
                },
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "extractMetadata"
                }
            ]
        }  
        """
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponsecontent(200, connectionpayload)

        modeler = ReplicationsModeler(cluster)

        cut = ModelerReplication('someName', modeler)
        self.assertIsNone(cluster.session.lastcalledurl)

        targetspace = cut.set_targetspace('HANA_CLOUD', '/SYSTEM')
        self.assertIsNotNone(targetspace)
        self.assertEqual(targetspace, cut.targetspace)
        self.assertEqual('HANA_CLOUD', cut.targetspace._connectionid)
        self.assertEqual('/SYSTEM', cut.targetspace._container)
        self.assertIsNotNone(cluster.session.lastcalledurl)
        self.assertEqual('HANA_DB', cut.targetspace._connectiontype)
        self.assertEqual('HANA', cut.targetspace._ccmconnectiontype)

    def test_save(self):
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponse(200)

        modeler = ReplicationsModeler(cluster)

        cut = ModelerReplication('ABCDEFGH123', modeler)
        cut._version = "ONE_SOURCE_ONE_TARGET"
        cut.save()

        self.assertIsNotNone(cluster.session.lastcalledurl)
        self.assertIn('files/rms/ABCDEFGH123.replication',
                      cluster.session.lastcalledurl)
        self.assertIsNotNone(cluster.session.posteddata)
        payload = json.loads(cluster.session.posteddata)
        self.assertEqual('ABCDEFGH123', payload['name'])

    def test_deploy(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        modeler = ReplicationsModeler(cluster)

        cut = ModelerReplication('ABAP_CDS_S4H_to_HC_deltaSkinny', modeler)
        cut.set_description('test')
        cut._version = "ONE_SOURCE_ONE_TARGET"

        sessionmock.setresponsecontent(200, """
        {
            "id": "S4H_2021",
            "description": "S4H 2021 - via QOI-927 (GAERTNERNI) ",
            "type": "ABAP",
            "ccmId": "663a485a-b7b6-48df-bc22-34d0527c7961",
            "ccmTypeId": "ABAP",
            "typeServices": [
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "extractMetadata"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "com.sap.dh.connection::statusCheck"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "browse"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "metadata"
                },
                {
                    "provider": "com.sap.dh.flowagent",
                    "type": "dataPreview"
                }
            ]
        }
        """)

        cut.set_sourcespace('S4H_2021', '/CDS')
        cut.sourcespace._connectiontype = 'ABAP'
        cut.sourcespace._ccmconnectiontype = 'ABAP'

        sessionmock.setresponsecontent(200,  """
        {
            "id": "HANA_CLOUD",
            "description": "CIT HANA CLOUD: 87cc15ab-65ef-4b28-9433-962f7400aecc.hana.canary-eu10.hanacloud.ondemand.com:443",
            "type": "HANA_DB",
            "readOnly": false,
            "ccmId": "44ff8f05-9e9c-4c97-abe2-c0260db87f51",
            "ccmTypeId": "HANA",
            "typeServices": [
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "rulesRemoteSource"
                },
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "extractLineage"
                },
                {
                    "provider": "com.sap.dh.metadata",
                    "type": "extractMetadata"
                }
            ]
        }  
        """)
        cut.set_targetspace('HANA_CLOUD', '/SYSTEM')
        cut.targetspace._connectiontype = 'HANA_DB'
        cut.targetspace._ccmconnectiontype = 'HANA'

        task = cut.create_task('Z_SEPM_I_SALESORDER')
        task.set_targetdataset(
            'CIT_ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER')

        # only for testing! should not be done in 'real' code!
        task._values['name'] = 'ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER_4ztxt9'

        sessionmock.setresponsecontent(
            202, '{"url": "/api/dt/v1/replicationflows/E2Etest008/changerequeststatus"}')
        replicationflow = cut.deploy()
        self.assertTrue(
            'app/rms/api/dt/v1/replicationflows' in sessionmock.lastcalledurl)
        self.assertIsNotNone(replicationflow)

        jsonContent = sessionmock.posteddata
        verficationContent = ModelerReplicationJson.serialize(cut)

        self.assertEquals(verficationContent, jsonContent)

    def test_undeploy(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock
        modeler = ReplicationsModeler(cluster)
        cut = ModelerReplication('ABAP_CDS_S4H_to_HC_TEST', modeler)
        sessionmock.setresponsecontent(
            202, '{"url": "/api/dt/v1/replicationflows/ABAP_CDS_S4H_to_HC_TEST/changerequeststatus"}')
        replicationflow = cut.undeploy()
        self.assertIsNotNone(replicationflow)
        self.assertEqual(replicationflow._name, 'ABAP_CDS_S4H_to_HC_TEST')
        self.assertTrue(
            'app/rms/api/dt/v1/replicationflows' in sessionmock.lastcalledurl)


class testModelerReplicationJson(unittest.TestCase):
    def getEmptyReplicationJson(self):
        return """
        {
            "name": "ABCDEFGH123", 
            "description": "", 
            "version": "ONE_SOURCE_ONE_TARGET", 
            "sourceSpaces": [
                {
                    "name": "", 
                    "connectionId": "", 
                    "connectionType": "", 
                    "technicalName": "", 
                    "ccmConnectionId": "", 
                    "ccmConnectionType": "", 
                    "container": ""
                }
            ], 
            "targetSpaces": [
                {
                    "name": "", 
                    "connectionId": "", 
                    "connectionType": "", 
                    "technicalName": "", 
                    "ccmConnectionId": "", 
                    "ccmConnectionType": "", 
                    "container": ""
                }
            ], 
            "oneSourceOneTargetTasks": []
        }
        """

    def getFilledSpacesReplicationJson(self):
        return """
        {
            "name": "ABAP_CDS_S4H_to_HC_deltaSkinny", 
            "description": "test", 
            "version": "ONE_SOURCE_ONE_TARGET", 
            "sourceSpaces": [
                {
                    "name": "ABAP_CDS_S4H_to_HC_deltaSkinny_S4H_2021_src", 
                    "connectionId": "S4H_2021", 
                    "connectionType": "ABAP", 
                    "technicalName": "S4H_2021", 
                    "ccmConnectionId": "S4H_2021", 
                    "ccmConnectionType": "ABAP", 
                    "container": "/CDS"
                }
            ], 
            "targetSpaces": [
                {
                    "name": "ABAP_CDS_S4H_to_HC_deltaSkinny_HANA_CLOUD_tgt", 
                    "connectionId": "HANA_CLOUD", 
                    "connectionType": "HANA_DB", 
                    "technicalName": "HANA_CLOUD", 
                    "ccmConnectionId": "HANA_CLOUD", 
                    "ccmConnectionType": "HANA", 
                    "container": "/SYSTEM"
                }
            ], 
            "oneSourceOneTargetTasks": []
        }
        """

    def getCompleteReplicationJson(self):
        return """
        {
            "name": "ABAP_CDS_S4H_to_HC_deltaSkinny", 
            "description": "test", 
            "version": "ONE_SOURCE_ONE_TARGET", 
            "sourceSpaces": [
                {
                    "name": "ABAP_CDS_S4H_to_HC_deltaSkinny_S4H_2021_src", 
                    "connectionId": "S4H_2021", 
                    "connectionType": "ABAP", 
                    "technicalName": "S4H_2021", 
                    "ccmConnectionId": "S4H_2021", 
                    "ccmConnectionType": "ABAP", 
                    "container": "/CDS"
                }
            ], 
            "targetSpaces": [
                {
                    "name": "ABAP_CDS_S4H_to_HC_deltaSkinny_HANA_CLOUD_tgt", 
                    "connectionId": "HANA_CLOUD", 
                    "connectionType": "HANA_DB", 
                    "technicalName": "HANA_CLOUD", 
                    "ccmConnectionId": "HANA_CLOUD", 
                    "ccmConnectionType": "HANA", 
                    "container": "/SYSTEM"
                }
            ], 
            "oneSourceOneTargetTasks": [
                {
                    "name": "ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER_4ztxt9", 
                    "description": "", 
                    "sourceDataset": "Z_SEPM_I_SALESORDER", 
                    "sourceSpace": "ABAP_CDS_S4H_to_HC_deltaSkinny_S4H_2021_src", 
                    "targetDataset": "CIT_ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER", 
                    "targetSpace": "ABAP_CDS_S4H_to_HC_deltaSkinny_HANA_CLOUD_tgt", 
                    "filter": [], 
                    "mappings": [], 
                    "loadType": "INITIAL", 
                    "truncate": false
                }
            ]
        }
        """

    def test_serializeEmpty(self):
        replication = Replication('ABCDEFGH123')
        replication._version = "ONE_SOURCE_ONE_TARGET"

        jsonContent = ModelerReplicationJson.serialize(replication)
        verficationContent = self.getEmptyReplicationJson()

        self.assertEqual(json.loads(verficationContent),
                         json.loads(jsonContent))

    def test_serializeFilled(self):
        replication = Replication('ABAP_CDS_S4H_to_HC_deltaSkinny')
        replication.set_description('test')
        replication._version = "ONE_SOURCE_ONE_TARGET"

        replication.set_sourcespace('S4H_2021', '/CDS')
        replication.sourcespace._connectiontype = 'ABAP'
        replication.sourcespace._ccmconnectiontype = 'ABAP'

        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        replication.targetspace._connectiontype = 'HANA_DB'
        replication.targetspace._ccmconnectiontype = 'HANA'

        jsonContent = ModelerReplicationJson.serialize(replication)
        verficationContent = self.getFilledSpacesReplicationJson()

        self.assertEqual(json.loads(verficationContent),
                         json.loads(jsonContent))

    def test_serializeFilledWithTask(self):
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
        #
        task._values['name'] = 'ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER_4ztxt9'

        task.set_loadtype(ReplicationLoadtype.INITIAL)

        jsonContent = ModelerReplicationJson.serialize(replication)
        verficationContent = self.getCompleteReplicationJson()

        self.assertEqual(json.loads(verficationContent),
                         json.loads(jsonContent))

    def test_serializeFilledWithDeltaTask(self):
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
        #
        task._values['name'] = 'ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER_4ztxt9'

        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        jsonContent = ModelerReplicationJson.serialize(replication)
        verficationContent = self.getCompleteReplicationJson()

        verificationStructure = json.loads(verficationContent)
        verificationStructure['oneSourceOneTargetTasks'][0]['loadType'] = ReplicationLoadtype.INITIAL_AND_DELTA.value

        self.assertEqual(verificationStructure,
                         json.loads(jsonContent))

    def test_serializeFilledWithFiltersTask(self):
        replication = Replication('ABAP_SLT_S4H_to_HC_initFat')
        replication._version = "ONE_SOURCE_ONE_TARGET"

        replication.set_sourcespace('S4H_2021', '/SLT/6FS')
        replication.sourcespace._connectiontype = 'ABAP'
        replication.sourcespace._ccmconnectiontype = 'ABAP'

        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        replication.targetspace._connectiontype = 'HANA_DB'
        replication.targetspace._ccmconnectiontype = 'HANA'

        task = replication.create_task('ACDOCA')
        task.set_targetdataset(
            'CIT_ABAP_SLT_S4H_to_HC_initFat_ACDOCA')
        #
        task._values['name'] = 'ABAP_SLT_S4H_to_HC_initFat_ACDOCA_psy5id'

        task._filters.add_filter(
            'RLDNR', ReplicationTaskFilterOperator.EQUALS, '0D')
        task._filters.add_filter(
            'GJAHR', ReplicationTaskFilterOperator.EQUALS, '2013')

        actualJson = ModelerReplicationJson.serialize(replication)
        expectedJson = self.getCompleteReplicationWithFiltersJson()

        expectedStructure = json.loads(expectedJson)

        actualStructure = json.loads(actualJson)
        self.assertEqual(expectedStructure,
                         actualStructure)

    def getCompleteReplicationWithFiltersJson(self):
        return """
        {
            "name": "ABAP_SLT_S4H_to_HC_initFat",
            "description": "",
            "version": "ONE_SOURCE_ONE_TARGET",
            "sourceSpaces": [
                {
                    "name": "ABAP_SLT_S4H_to_HC_initFat_S4H_2021_src",
                    "connectionId": "S4H_2021",
                    "connectionType": "ABAP",
                    "technicalName": "S4H_2021",
                    "ccmConnectionId": "S4H_2021",
                    "ccmConnectionType": "ABAP",
                    "container": "/SLT/6FS"
                }
            ],
            "targetSpaces": [
                {
                    "name": "ABAP_SLT_S4H_to_HC_initFat_HANA_CLOUD_tgt",
                    "connectionId": "HANA_CLOUD",
                    "connectionType": "HANA_DB",
                    "technicalName": "HANA_CLOUD",
                    "ccmConnectionId": "HANA_CLOUD",
                    "ccmConnectionType": "HANA",
                    "container": "/SYSTEM"
                }
            ],
            "oneSourceOneTargetTasks": [
                {
                    "name": "ABAP_SLT_S4H_to_HC_initFat_ACDOCA_psy5id",
                    "description": "",
                    "sourceDataset": "ACDOCA",
                    "sourceSpace": "ABAP_SLT_S4H_to_HC_initFat_S4H_2021_src",
                    "targetDataset": "CIT_ABAP_SLT_S4H_to_HC_initFat_ACDOCA",
                    "targetSpace": "ABAP_SLT_S4H_to_HC_initFat_HANA_CLOUD_tgt",
                    "filter": [
                        { "name": "RLDNR", "elements": [{ "comparison": "=", "low": "0D" }] },
                        { "name": "GJAHR", "elements": [{ "comparison": "=", "low": "2013" }] }
                    ],
                    "mappings": [],
                    "loadType": "INITIAL",
                    "truncate": false
                }
            ]
        }
        """

    def test_serializeFilledWithTargetspaceProperties(self):
        replication = Replication('cloudFStest')
        replication._description = 'A description'
        replication._version = "ONE_SOURCE_ONE_TARGET"

        replication.set_sourcespace('UK5', '/SLT/3LK')
        replication.sourcespace._connectiontype = 'ABAP'
        replication.sourcespace._ccmconnectiontype = 'ABAP'

        targetspace = replication.set_targetspace('S3_CIT', '/D050579/Test1')
        replication.targetspace._connectiontype = 'S3'
        replication.targetspace._ccmconnectiontype = 'S3'

        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.GZIP)

        task = replication.create_task('SNWD_SO_I')
        #
        task._values['name'] = 'cloudFStest_SNWD_SO_I_h5zpbt'

        task._filters.add_filter(
            'SO_ITEM_POS', ReplicationTaskFilterOperator.EQUALS, '0000000100')

        actualJson = ModelerReplicationJson.serialize(replication)
        expectedJson = get_replication_with_targetspace_properties()

        expectedStructure = json.loads(expectedJson)

        actualStructure = json.loads(actualJson)
        self.assertEqual(expectedStructure,
                         actualStructure)

    def test_deserialize_FilledWithTargetspaceProperties(self):
        json_data = get_replication_with_targetspace_properties()
        replication = ModelerReplicationJson.deserialize(json_data)
        self.assertIsNotNone(replication)
        self.assertEqual('cloudFStest', replication.name)
        self.assertEqual('A description', replication.description)
        self.assertEqual('ONE_SOURCE_ONE_TARGET', replication.version)

        self.assertIsNotNone(replication.sourcespace)
        self.assertEqual('UK5', replication.sourcespace._connectionid)
        self.assertEqual('/SLT/3LK', replication.sourcespace._container)
        self.assertEqual('ABAP', replication.sourcespace._connectiontype)
        self.assertEqual('ABAP', replication.sourcespace._ccmconnectiontype)

        self.assertIsNotNone(replication.targetspace)
        self.assertEqual('S3_CIT', replication.targetspace._connectionid)
        self.assertEqual('/D050579/Test1', replication.targetspace._container)
        self.assertEqual('S3', replication.targetspace._connectiontype)
        self.assertEqual('S3', replication.targetspace._ccmconnectiontype)
        self.assertEqual(ReplicationSpaceGroupDeltaBy.DATE.value,
                         replication.targetspace._datasetProperties[ReplicationSpaceProperty.GROUP_DELTA_BY.value])
        self.assertEqual(ReplicationSpaceFileType.PARQUET.value,
                         replication.targetspace._datasetProperties[ReplicationSpaceProperty.FILE_TYPE.value])
        self.assertEqual(ReplicationSpaceFileCompression.GZIP.value,
                         replication.targetspace._datasetProperties[ReplicationSpaceProperty.FILE_COMPRESSION.value])

        self.assertIsNotNone(replication.tasks)
        self.assertEqual(1, len(replication.tasks))
        self.assertEqual('cloudFStest_SNWD_SO_I_h5zpbt',
                         replication.tasks[0].name)
        self.assertEqual('', replication.tasks[0].description)
        self.assertEqual('SNWD_SO_I', replication.tasks[0].sourcedataset)
        self.assertEqual('cloudFStest_UK5_src',
                         replication.tasks[0]._values['sourceSpace'])
        self.assertEqual('SNWD_SO_I', replication.tasks[0].targetdataset)
        self.assertEqual('cloudFStest_S3_CIT_tgt',
                         replication.tasks[0]._values['targetSpace'])

        self.assertIsNotNone(replication.tasks[0]._values['filter'])
        self.assertEqual(1, len(replication.tasks[0]._values['filter']))
        filter = replication.tasks[0]._values['filter'][0]
        self.assertEqual('SO_ITEM_POS', filter['name'])
        self.assertEqual(1, len(filter['elements']))

        self.assertEqual(0, len(replication.tasks[0]._values['mappings']))
        self.assertEqual('INITIAL', replication.tasks[0]._values['loadType'])
        self.assertEqual(False, replication.tasks[0]._values['truncate'])

    def test_roudtrip_basic(self):
        expectedJson = self.getCompleteReplicationJson()
        replication = ModelerReplicationJson.deserialize(expectedJson)
        actualJson = ModelerReplicationJson.serialize(replication)

        expectedStructure = json.loads(expectedJson)

        actualStructure = json.loads(actualJson)
        self.assertEqual(expectedStructure,
                         actualStructure)

    def test_roudtrip_FilledWithTargetspaceProperties(self):
        expectedJson = get_replication_with_targetspace_properties()
        replication = ModelerReplicationJson.deserialize(expectedJson)
        actualJson = ModelerReplicationJson.serialize(replication)

        expectedStructure = json.loads(expectedJson)

        actualStructure = json.loads(actualJson)
        self.assertEqual(expectedStructure,
                         actualStructure)

    def test_deserialize_modelerreplication(self):
        cluster = Cluster(getDummyConnectionData())
        cluster.session = SessionMock()
        cluster.session.setresponse(200)
        modeler = Modeler(cluster)
        replications = modeler.replications
        json_data = get_replication_with_targetspace_properties()
        replication = ModelerReplicationJson.deserialize(
            json_data, replications)
        self.assertIsInstance(replication, ModelerReplication)


def get_replication_with_targetspace_properties():
    return """
    {
        "name": "cloudFStest",
        "description": "A description",
        "version": "ONE_SOURCE_ONE_TARGET",
        "sourceSpaces": [
            {
                "name": "cloudFStest_UK5_src",
                "connectionId": "UK5",
                "connectionType": "ABAP",
                "technicalName": "UK5",
                "ccmConnectionId": "UK5",
                "ccmConnectionType": "ABAP",
                "container": "/SLT/3LK"
            }
        ],
        "targetSpaces": [
            {
                "name": "cloudFStest_S3_CIT_tgt",
                "connectionId": "S3_CIT",
                "connectionType": "S3",
                "technicalName": "S3_CIT",
                "ccmConnectionId": "S3_CIT",
                "ccmConnectionType": "S3",
                "container": "/D050579/Test1",
                "datasetProperties": {
                    "groupDeltaFilesBy": "DATE",
                    "format": "PARQUET",
                    "compression": "GZIP"
                }
            }
        ],
        "oneSourceOneTargetTasks": [
            {
                "name": "cloudFStest_SNWD_SO_I_h5zpbt",
                "description": "",
                "sourceDataset": "SNWD_SO_I",
                "sourceSpace": "cloudFStest_UK5_src",
                "targetDataset": "SNWD_SO_I",
                "targetSpace": "cloudFStest_S3_CIT_tgt",
                "filter": [
                    {
                        "name": "SO_ITEM_POS",
                        "elements": [{ "comparison": "=", "low": "0000000100" }]
                    }
                ],
                "mappings": [],
                "loadType": "INITIAL",
                "truncate": false
            }
        ]
    }        
    """


if __name__ == '__main__':
    unittest.main()
