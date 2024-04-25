from datetime import datetime
import unittest
import json

from framework.infrastructure.Cluster import Cluster, ClusterConnectionData
from framework.infrastructure.Monitoring import Monitoring, ReplicationMonitor, ReplicationMonitorTaskMetrics, Replicationtaskmonitor, ReplicationsMonitoring, ReplicationtaskmonitorPartition, ReplicationtaskmonitorStatus

from framework.unittests.doubles.SessionMock import SessionMock


def getDummyConnectionData():
    connectionData = ClusterConnectionData('POD-INT', 'https://cluster')
    connectionData.tenant = 'default'
    connectionData.user = 'tester'
    connectionData.password = '********'
    return connectionData


class testMonitoring(unittest.TestCase):

    def test_skeleton(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        cut = Monitoring(cluster)

        replications = cut.replications

        self.assertIsNotNone(replications)


class testReplicationsMonitoring(unittest.TestCase):

    def test_skeleton(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        monitoring = Monitoring(cluster)

        cut = ReplicationsMonitoring(monitoring)

        sessionmock.setresponsecontent(
            200, getReplicationflowmonitorsDummyResponse())
        monitors = cut.get_monitors()
        self.assertTrue(sessionmock.lastcalledurl.endswith(
            '/app/rms/api/dt/v1/replicationflowMonitors'))
        self.assertIsNotNone(monitors)
        self.assertEqual(4, len(monitors))
        self.assertEqual('PERFORMANCE', monitors[0].name)

    def test_getmonitor(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        monitoring = Monitoring(cluster)

        cut = ReplicationsMonitoring(monitoring)

        sessionmock.setresponsecontent(
            200, getReplicationflowmonitorsDummySingleResponse())
        monitor = cut.get_monitor('abap-s4hc-to-hc')
        self.assertEqual('abap-s4hc-to-hc', monitor.name)

    def test_gettaskmonitor(self):
        cluster = Cluster(getDummyConnectionData())
        sessionmock = SessionMock()
        cluster.session = sessionmock

        monitoring = Monitoring(cluster)

        cut = ReplicationsMonitoring(monitoring)

        sessionmock.setresponsecontent(
            200, getReplicationtaskmonitorsDummyResponse())
        taskmonitors = cut.get_taskmonitors('E2ESLTtest008')
        self.assertIsNotNone(taskmonitors)
        self.assertEqual(1, len(taskmonitors))
        self.assertTrue(sessionmock.lastcalledurl.endswith(
            '/app/rms/api/dt/v1/replicationflows/E2ESLTtest008/taskMonitors'))


class testReplicationMonitor(unittest.TestCase):

    def test_skeleton(self):
        jsondata = getReplicationflowmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        cut = ReplicationMonitor(monitors[0])
        self.assertEqual('PERFORMANCE', cut.name)
        self.assertIsNone(cut.status)

    def test_getstatus(self):
        jsondata = getReplicationflowmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        cut = ReplicationMonitor(monitors[1])
        self.assertEqual('RUNNING', cut.status)
        cut = ReplicationMonitor(monitors[3])
        self.assertEqual('CREATED', cut.status)

    def test_gettaskmetrics(self):
        jsondata = getReplicationflowmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        cut = ReplicationMonitor(monitors[0])
        taskmetrics = cut.taskmetrics
        self.assertIsNone(taskmetrics)
        cut = ReplicationMonitor(monitors[1])
        taskmetrics = cut.taskmetrics
        self.assertIsNotNone(taskmetrics)

    def test_str(self):
        jsondata = getReplicationflowmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        cut = ReplicationMonitor(monitors[0])
        taskmetrics = cut.taskmetrics
        self.assertIsNone(taskmetrics)
        cut = ReplicationMonitor(monitors[1])
        self.assertEqual(
            'abap-s4hc-to-hc:\tRUNNING - Total:\t1, Error:\t0, Completed:\t1, Initial completed:\t1, Created:\t0', str(cut))


class testReplicationMonitoriTaskMetric(unittest.TestCase):

    def test_skeleton(self):
        jsondata = getReplicationflowmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        monitor = ReplicationMonitor(monitors[1])
        cut = ReplicationMonitorTaskMetrics(monitor._values['taskMetrics'])
        self.assertEqual(1, cut.total)
        self.assertEqual(1, cut.completed)
        self.assertEqual(0, cut.error)
        self.assertEqual(1, cut.initial_completed)
        self.assertEqual(0, cut.created)
        monitor = ReplicationMonitor(monitors[2])
        cut = ReplicationMonitorTaskMetrics(monitor._values['taskMetrics'])
        self.assertEqual(1, cut.total)
        self.assertEqual(0, cut.completed)
        self.assertEqual(1, cut.error)
        self.assertEqual(0, cut.initial_completed)
        self.assertEqual(0, cut.created)
        monitor = ReplicationMonitor(monitors[3])
        cut = ReplicationMonitorTaskMetrics(monitor._values['taskMetrics'])
        self.assertEqual(1, cut.total)
        self.assertEqual(0, cut.completed)
        self.assertEqual(0, cut.error)
        self.assertEqual(0, cut.initial_completed)
        self.assertEqual(1, cut.created)

    def test_str(self):
        jsondata = getReplicationflowmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        monitor = ReplicationMonitor(monitors[1])
        cut = ReplicationMonitorTaskMetrics(monitor._values['taskMetrics'])
        self.assertEqual(
            'Total:\t1, Error:\t0, Completed:\t1, Initial completed:\t1, Created:\t0', str(cut))
        monitor = ReplicationMonitor(monitors[2])
        cut = ReplicationMonitorTaskMetrics(monitor._values['taskMetrics'])
        self.assertEqual(
            'Total:\t1, Error:\t1, Completed:\t0, Initial completed:\t0, Created:\t0', str(cut))
        monitor = ReplicationMonitor(monitors[3])
        cut = ReplicationMonitorTaskMetrics(monitor._values['taskMetrics'])
        self.assertEqual(
            'Total:\t1, Error:\t0, Completed:\t0, Initial completed:\t0, Created:\t1', str(cut))


class testReplicationtaskmonitor(unittest.TestCase):

    def test_basiscs(self):
        jsondata = getReplicationtaskmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])

        self.assertEqual('E2ESLTtest008_ZTS_COUNTRY_91dtkd', cut.name)

    def test_getstatus(self):
        jsondata = getReplicationtaskmonitorsErrorDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])

        self.assertEqual(ReplicationtaskmonitorStatus.ERROR, cut.status)

        jsondata = getReplicationtaskmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])

        self.assertEqual(
            ReplicationtaskmonitorStatus.COMPLETED, cut.status)

    def test_getstatusinfo(self):
        jsondata = getReplicationtaskmonitorsErrorDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])

        self.assertTrue(cut.statusinfo.startswith(
            'ape[RFC] get connection: [RFC] error open connection: Openi'))

    def test_partitions(self):
        jsondata = getReplicationtaskmonitorsWithMultipleTasksDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])
        partitions = cut.partitions
        self.assertEqual(4, len(partitions))
        self.assertEqual('42010AEE1BB61EEC92894F1B65D1DD23', partitions[0].id)
        self.assertEqual(170, partitions[0].transformRecordCount)
        self.assertEqual(349, partitions[0].targetProcessingTime)

    def test_empty_partitions(self):
        jsondata = getReplicationtaskmonitorsErrorDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])
        partitions = cut.partitions
        self.assertEqual(0, len(partitions))

    def test_numberOfRecordsTransferred(self):
        jsondata = getReplicationtaskmonitorsWithMultipleTasksDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])
        self.assertEqual(170, cut.numberOfRecordsTransferred)
        cut = Replicationtaskmonitor(monitors[1])
        self.assertEqual(1061, cut.numberOfRecordsTransferred)

    def test_startTime(self):
        jsondata = getReplicationtaskmonitorsWithMultipleTasksDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])
        expected_start_time = datetime(
            year=2021, month=11, day=18, hour=8, minute=35, second=4, microsecond=254312)
        self.assertEqual(expected_start_time, cut.startTime)

    def test_lastRuntimeUpdated(self):
        jsondata = getReplicationtaskmonitorsWithMultipleTasksDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])
        expected_last_runtime_update = datetime(
            year=2021, month=11, day=26, hour=3, minute=51, second=49, microsecond=725715)
        self.assertEqual(expected_last_runtime_update, cut.lastRuntimeUpdated)

    def test_initialLoadEndTime(self):
        jsondata = getReplicationtaskmonitorsDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])
        expected_initial_load_endtime = datetime(
            year=2021, month=11, day=24, hour=15, minute=32, second=28, microsecond=592302)
        self.assertEqual(expected_initial_load_endtime, cut.initialLoadEndTime)

    def test_initialLoadEndTime_missing(self):
        jsondata = getReplicationtaskmonitorsErrorDummyResponse()
        monitors = json.loads(jsondata)
        cut = Replicationtaskmonitor(monitors[0])
        expected_initial_load_endtime = None
        self.assertEqual(expected_initial_load_endtime, cut.initialLoadEndTime)


class testReplicationtaskmonitorPartition(unittest.TestCase):

    def test_basics(self):
        id = '1234567890ABCDEF'
        data = {}
        data['partitionMetrics'] = {}
        data['statusInfo'] = 'Status information of partition.'
        cut = ReplicationtaskmonitorPartition(id, data)
        self.assertEqual('1234567890ABCDEF', cut.id)
        self.assertEqual('Status information of partition.', cut.statusInfo)

    def test_metrics(self):
        data = {}
        metrics = {}
        id = '1234567890ABCDEF'
        data['partitionMetrics'] = metrics

        metrics['firstActivatedAt'] = '2022-04-27T08:05:16.2454626'
        metrics['lastAccessedAt'] = '2022-04-27T08:05:17.2454626'
        metrics['lastRetriedAt'] = '2022-04-27T08:05:18.2454626'
        metrics['lastErrorAt'] = '2022-04-27T08:05:19.2454626'
        metrics['completedAt'] = '2022-04-27T08:05:20.2454626'

        metrics['sourceBytes'] = 22400
        metrics['sourceRecordCount'] = 100
        metrics['sourceProcessingTime'] = 71
        metrics['transformBytes'] = 9064
        metrics['transformRecordCount'] = 100
        metrics['targetBytes'] = 15649
        metrics['targetRecordCount'] = 100
        metrics['targetProcessingTime'] = 221

        cut = ReplicationtaskmonitorPartition(id, data)

        expected_firstActivatedAt = datetime(
            year=2022, month=4, day=27, hour=8, minute=5, second=16, microsecond=245463)
        self.assertEqual(expected_firstActivatedAt, cut.firstActivatedAt)

        expected_lastAccessedAt = datetime(
            year=2022, month=4, day=27, hour=8, minute=5, second=17, microsecond=245463)
        self.assertEqual(expected_lastAccessedAt, cut.lastAccessedAt)

        expected_lastRetriedAt = datetime(
            year=2022, month=4, day=27, hour=8, minute=5, second=18, microsecond=245463)
        self.assertEqual(expected_lastRetriedAt, cut.lastRetriedAt)

        expected_lastErrorAt = datetime(
            year=2022, month=4, day=27, hour=8, minute=5, second=19, microsecond=245463)
        self.assertEqual(expected_lastErrorAt, cut.lastErrorAt)

        expected_completedAt = datetime(
            year=2022, month=4, day=27, hour=8, minute=5, second=20, microsecond=245463)
        self.assertEqual(expected_completedAt, cut.completedAt)

        self.assertEqual(22400, cut.sourceBytes)
        self.assertEqual(100, cut.sourceRecordCount)
        self.assertEqual(71, cut.sourceProcessingTime)
        self.assertEqual(9064, cut.transformBytes)
        self.assertEqual(100, cut.transformRecordCount)
        self.assertEqual(15649, cut.targetBytes)
        self.assertEqual(100, cut.targetRecordCount)
        self.assertEqual(221, cut.targetProcessingTime)

    def test_empty_metrics(self):
        data = {}
        metrics = {}
        id = '1234567890ABCDEF'
        data['partitionMetrics'] = metrics

        cut = ReplicationtaskmonitorPartition(id, data)

        self.assertEqual(0, cut.sourceBytes)
        self.assertEqual(0, cut.sourceRecordCount)
        self.assertEqual(0, cut.sourceProcessingTime)
        self.assertEqual(0, cut.transformBytes)
        self.assertEqual(0, cut.transformRecordCount)
        self.assertEqual(0, cut.targetBytes)
        self.assertEqual(0, cut.targetRecordCount)
        self.assertEqual(0, cut.targetProcessingTime)


class testReplicationtaskmonitorStatus(unittest.TestCase):

    def test_basics(self):
        cut = ReplicationtaskmonitorStatus.CREATED
        cut = ReplicationtaskmonitorStatus.INITIAL_RUNNING
        cut = ReplicationtaskmonitorStatus.DELTA_RUNNING
        cut = ReplicationtaskmonitorStatus.SUSPENDING
        cut = ReplicationtaskmonitorStatus.SUSPENDED
        cut = ReplicationtaskmonitorStatus.RETRYING
        cut = ReplicationtaskmonitorStatus.COMPLETED
        cut = ReplicationtaskmonitorStatus.ERROR


def getReplicationflowmonitorsDummyResponse():
    return """
[
  {
    "name": "PERFORMANCE",
    "sourceSpaces": [
      {
        "name": "PERFORMANCE_ODE_src",
        "connectionId": "ODE",
        "connectionType": "ABAP",
        "technicalName": "ODE",
        "ccmConnectionId": "ODE",
        "ccmConnectionType": "ABAP",
        "container": "/CDS"
      }
    ],
    "targetSpaces": [
      {
        "name": "PERFORMANCE_BW_HANA_tgt",
        "connectionId": "BW_HANA",
        "connectionType": "HANA_DB",
        "technicalName": "BW_HANA",
        "ccmConnectionId": "BW_HANA",
        "ccmConnectionType": "HANA",
        "container": "/RMS_TEST"
      }
    ],
    "objectChangedAt": "2021-09-27T07:58:57.8581814",
    "objectChangedBy": "d060045",
    "objectCreatedAt": "2021-09-27T06:35:40.4750814",
    "objectCreatedBy": "d060045",
    "configurationChangedAt": "",
    "runtimeChangedAt": ""
  },
  {
    "name": "abap-s4hc-to-hc",
    "sourceSpaces": [
      {
        "name": "abap-s4hc-to-hc_S4HANA_CLOUD_APE_src",
        "connectionId": "S4HANA_CLOUD_APE",
        "connectionType": "ABAP",
        "technicalName": "S4HANA_CLOUD_APE",
        "ccmConnectionId": "S4HANA_CLOUD_APE",
        "ccmConnectionType": "ABAP",
        "container": "/CDS"
      }
    ],
    "targetSpaces": [
      {
        "name": "abap-s4hc-to-hc_HANA_CLOUD_tgt",
        "connectionId": "HANA_CLOUD",
        "connectionType": "HANA_DB",
        "technicalName": "HANA_CLOUD",
        "ccmConnectionId": "HANA_CLOUD",
        "ccmConnectionType": "HANA",
        "container": "/RMS_CIT_TARGET"
      }
    ],
    "status": "RUNNING",
    "taskMetrics": { "completed": 1, "initialCompleted": 1, "total": 1 },
    "objectChangedAt": "2021-11-22T11:22:24.9918875",
    "objectChangedBy": "d060045",
    "objectCreatedAt": "2021-11-22T11:22:24.9918875",
    "objectCreatedBy": "d060045",
    "configurationChangedAt": "",
    "runtimeChangedAt": "2021-11-22T11:22:58.3156753",
    "statusMetrics": {
      "createdAt": "2021-11-22T11:22:22.7469131",
      "priorityChangedAt": "2021-11-22T11:22:22.7469131",
      "priorityChangedBy": "d060045",
      "firstActivatedAt": "2021-11-22T11:22:32.9982207",
      "lastActivatedAt": "2021-11-22T11:22:32.9982207",
      "lastActivatedBy": "d060045",
      "lastSuspendedAt": "2021-11-22T11:22:22.7469131",
      "lastSuspendedBy": "d060045",
      "timeSpentActive": 187513423,
      "activeCount": 1,
      "timeSpentSuspended": 10251,
      "suspendedCount": 1,
      "runtimeChangedAt": "2021-11-22T11:22:58.3156753",
      "spaceMetrics": {
        "abap-s4hc-to-hc_HANA_CLOUD_tgt": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-11-22T11:22:22.2265252",
          "maxConnectionsChangedBy": "d060045"
        },
        "abap-s4hc-to-hc_S4HANA_CLOUD_APE_src": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-11-22T11:22:21.8203852",
          "maxConnectionsChangedBy": "d060045"
        }
      },
      "configChangedAt": ""
    }
  },
  {
    "name": "ABAP_SLT_S4H_to_HC_initFat",
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
    "status": "RUNNING",
    "taskMetrics": { "error": 1, "total": 1 },
    "objectChangedAt": "2021-10-29T10:14:43.8705810",
    "objectChangedBy": "d050579",
    "objectCreatedAt": "2021-10-29T10:14:43.8705810",
    "objectCreatedBy": "d050579",
    "configurationChangedAt": "",
    "runtimeChangedAt": "2021-11-19T09:42:44.9647100",
    "statusMetrics": {
      "createdAt": "2021-10-29T10:14:39.3222973",
      "priorityChangedAt": "2021-10-29T10:14:39.3222973",
      "priorityChangedBy": "d050579",
      "firstActivatedAt": "2021-10-29T10:16:20.1896470",
      "lastActivatedAt": "2021-11-09T08:14:05.5233679",
      "lastActivatedBy": "d050579",
      "lastSuspendedAt": "2021-10-29T10:14:39.3222973",
      "lastSuspendedBy": "d050579",
      "timeSpentActive": 2265086597,
      "activeCount": 2,
      "timeSpentSuspended": 100867,
      "suspendedCount": 1,
      "runtimeChangedAt": "2021-11-19T09:42:44.9647100",
      "spaceMetrics": {
        "ABAP_SLT_S4H_to_HC_initFat_HANA_CLOUD_tgt": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-10-29T10:14:38.8767682",
          "maxConnectionsChangedBy": "d050579"
        },
        "ABAP_SLT_S4H_to_HC_initFat_S4H_2021_src": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-10-29T10:14:38.3217876",
          "maxConnectionsChangedBy": "d050579"
        }
      },
      "configChangedAt": ""
    }
  },
  {
    "name": "test001",
    "sourceSpaces": [
      {
        "name": "test001_ODE_src",
        "connectionId": "ODE",
        "connectionType": "ABAP",
        "technicalName": "ODE",
        "ccmConnectionId": "ODE",
        "ccmConnectionType": "ABAP",
        "container": "/CDS"
      }
    ],
    "targetSpaces": [
      {
        "name": "test001_BW_HANA_tgt",
        "connectionId": "BW_HANA",
        "connectionType": "HANA_DB",
        "technicalName": "BW_HANA",
        "ccmConnectionId": "BW_HANA",
        "ccmConnectionType": "HANA",
        "container": "/SYSTEM"
      }
    ],
    "status": "CREATED",
    "taskMetrics": { "created": 1, "total": 1 },
    "objectChangedAt": "2021-11-18T01:39:35.7361336",
    "objectChangedBy": "d060045",
    "objectCreatedAt": "2021-11-18T01:39:35.7361336",
    "objectCreatedBy": "d060045",
    "configurationChangedAt": "",
    "runtimeChangedAt": "2021-11-18T01:39:35.5659455",
    "statusMetrics": {
      "createdAt": "2021-11-16T09:19:55.1188560",
      "priorityChangedAt": "2021-11-16T09:19:55.1188560",
      "priorityChangedBy": "d060045",
      "firstActivatedAt": "",
      "lastActivatedAt": "",
      "lastSuspendedAt": "2021-11-16T09:19:55.1188560",
      "lastSuspendedBy": "d060045",
      "timeSpentSuspended": 713272195,
      "suspendedCount": 1,
      "runtimeChangedAt": "2021-11-18T01:39:35.5659455",
      "spaceMetrics": {
        "test001_BW_HANA_tgt": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-11-16T09:18:43.0007080",
          "maxConnectionsChangedBy": "d060045"
        },
        "test001_ODE_src": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-11-16T09:17:11.3241168",
          "maxConnectionsChangedBy": "d060045"
        }
      },
      "configChangedAt": ""
    }
  }
]    
    """


def getReplicationflowmonitorsDummySingleResponse():
    return """
[
  {
    "name": "abap-s4hc-to-hc",
    "sourceSpaces": [
      {
        "name": "abap-s4hc-to-hc_S4HANA_CLOUD_APE_src",
        "connectionId": "S4HANA_CLOUD_APE",
        "connectionType": "ABAP",
        "technicalName": "S4HANA_CLOUD_APE",
        "ccmConnectionId": "S4HANA_CLOUD_APE",
        "ccmConnectionType": "ABAP",
        "container": "/CDS"
      }
    ],
    "targetSpaces": [
      {
        "name": "abap-s4hc-to-hc_HANA_CLOUD_tgt",
        "connectionId": "HANA_CLOUD",
        "connectionType": "HANA_DB",
        "technicalName": "HANA_CLOUD",
        "ccmConnectionId": "HANA_CLOUD",
        "ccmConnectionType": "HANA",
        "container": "/RMS_CIT_TARGET"
      }
    ],
    "status": "RUNNING",
    "taskMetrics": { "completed": 1, "initialCompleted": 1, "total": 1 },
    "objectChangedAt": "2021-11-22T11:22:24.9918875",
    "objectChangedBy": "d060045",
    "objectCreatedAt": "2021-11-22T11:22:24.9918875",
    "objectCreatedBy": "d060045",
    "configurationChangedAt": "",
    "runtimeChangedAt": "2021-11-22T11:22:58.3156753",
    "statusMetrics": {
      "createdAt": "2021-11-22T11:22:22.7469131",
      "priorityChangedAt": "2021-11-22T11:22:22.7469131",
      "priorityChangedBy": "d060045",
      "firstActivatedAt": "2021-11-22T11:22:32.9982207",
      "lastActivatedAt": "2021-11-22T11:22:32.9982207",
      "lastActivatedBy": "d060045",
      "lastSuspendedAt": "2021-11-22T11:22:22.7469131",
      "lastSuspendedBy": "d060045",
      "timeSpentActive": 187513423,
      "activeCount": 1,
      "timeSpentSuspended": 10251,
      "suspendedCount": 1,
      "runtimeChangedAt": "2021-11-22T11:22:58.3156753",
      "spaceMetrics": {
        "abap-s4hc-to-hc_HANA_CLOUD_tgt": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-11-22T11:22:22.2265252",
          "maxConnectionsChangedBy": "d060045"
        },
        "abap-s4hc-to-hc_S4HANA_CLOUD_APE_src": {
          "maxConnections": 10,
          "maxConnectionsChangedAt": "2021-11-22T11:22:21.8203852",
          "maxConnectionsChangedBy": "d060045"
        }
      },
      "configChangedAt": ""
    }
  }
]    
    """


def getReplicationtaskmonitorsDummyResponse():
    return """
[
  {
    "name": "E2ESLTtest008_ZTS_COUNTRY_91dtkd",
    "flowName": "E2ESLTtest008",
    "sourceDataset": "ZTS_COUNTRY",
    "targetDataset": "ZTS_COUNTRY",
    "priority": "MEDIUM",
    "loadType": "INITIAL",
    "status": "COMPLETED",
    "state": "Completed",
    "active": false,
    "numberOfRecordsTransferred": 255,
    "numberOfPartitions": 3,
    "lastRuntimeUpdated": "2021-11-24T15:32:28.5923021",
    "startTime": "2021-11-24T15:31:33.1158377",
    "initialLoadEndTime": "2021-11-24T15:32:28.5923021",
    "partitions": {
      "42010AEF3F5B1EDC93A77407A25CA0EF": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-24T15:31:43.8966986",
          "lastAccessedAt": "2021-11-24T15:32:13.0635111",
          "lastRetriedAt": "2021-11-24T15:31:58.2938883",
          "lastErrorAt": "",
          "completedAt": "2021-11-24T15:32:13.0635111",
          "timeSpentRetrying": 236,
          "retryCount": 2
        }
      },
      "42010AEF3F5B1EDC93A77407A25CC0EF": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-24T15:31:43.8966986",
          "lastAccessedAt": "2021-11-24T15:32:27.5128720",
          "lastRetriedAt": "2021-11-24T15:31:59.5401968",
          "lastErrorAt": "",
          "completedAt": "2021-11-24T15:32:27.5128720",
          "timeSpentRetrying": 349,
          "retryCount": 3
        }
      },
      "42010AEF3F5B1EDC93A77407A25CE0EF": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-24T15:31:43.8966986",
          "lastAccessedAt": "2021-11-24T15:32:28.5923021",
          "lastRetriedAt": "2021-11-24T15:31:56.3625252",
          "lastErrorAt": "",
          "completedAt": "2021-11-24T15:32:28.5923021",
          "timeSpentRetrying": 294,
          "retryCount": 2,
          "sourceBytes": 8670,
          "sourceRecordCount": 255,
          "sourceProcessingTime": 201,
          "transformBytes": 5527,
          "transformRecordCount": 255,
          "targetBytes": 8670,
          "targetRecordCount": 255,
          "targetProcessingTime": 1919
        }
      }
    },
    "initialLoadMetrics": {
      "priorityChangedAt": "2021-11-24T11:20:01.3420356",
      "priorityChangedBy": "d050579",
      "firstActivatedAt": "2021-11-24T15:31:33.1158377",
      "lastActivatedAt": "2021-11-24T15:31:33.1158377",
      "lastActivatedBy": "d050579",
      "lastSuspendedAt": "2021-11-24T11:20:01.3420356",
      "lastSuspendedBy": "d050579",
      "lastRetriedAt": "",
      "lastErrorAt": "",
      "lastAccessedAt": "2021-11-24T15:32:28.5923021",
      "completedAt": "2021-11-24T15:32:28.5923021",
      "timeSpentActive": 55476,
      "activeCount": 1,
      "timeSpentSuspended": 15091773,
      "suspendCount": 1
    }
  }
]
    """


def getReplicationtaskmonitorsErrorDummyResponse():
    return """
[
  {
    "name": "ABAP_SLT_S4H_to_HC_initSkinny_SNWD_SO_I_3w2mmp",
    "flowName": "ABAP_SLT_S4H_to_HC_initSkinny",
    "sourceDataset": "SNWD_SO_I",
    "targetDataset": "CIT_ABAP_SLT_S4H_to_HC_initSkinny_SNWD_SO_I",
    "priority": "MEDIUM",
    "loadType": "INITIAL",
    "status": "ERROR",
    "state": "Error",
    "active": false,
    "statusInfo": "ape[RFC] get connection: [RFC] error open connection: Opening connection to backend failed:  LOCATION    CPIC (TCP/IP) on local host ERROR       partner '10.239.63.91:sapgw00' not reached TIME        Tue Nov  9 18:06:06 2021 RELEASE     749 COMPONENT   NI (network interface) VERSION     40 RC          -10 MODULE      /bas/749_REL/src/base/ni/nixxi.cpp LINE        3428 DETAIL      NiPConnect2: 10.239.63.91:3300 SYSTEM CALL connect ERRNO       111 ERRNO TEXT  Connection refused COUNTER     66 . Please refer to SAP Note 2849542 for more information",
    "lastRuntimeUpdated": "2021-11-09T18:06:07.1579369",
    "startTime": "2021-10-29T10:06:02.4522898",
    "initialLoadMetrics": {
      "priorityChangedAt": "2021-10-29T10:05:46.3233662",
      "priorityChangedBy": "d050579",
      "firstActivatedAt": "2021-10-29T10:06:02.4522898",
      "lastActivatedAt": "2021-11-09T14:18:57.7228174",
      "lastActivatedBy": "d050579",
      "lastSuspendedAt": "2021-10-29T13:59:05.8619600",
      "lastSuspendedBy": "d050579",
      "lastRetriedAt": "2021-11-09T18:05:47.4293842",
      "lastErrorAt": "2021-11-09T18:06:07.1579369",
      "lastAccessedAt": "2021-11-09T18:06:07.1579369",
      "completedAt": "",
      "timeSpentActive": 5324264,
      "activeCount": 4,
      "timeSpentSuspended": 17408844,
      "suspendCount": 2,
      "timeSpentRetrying": 294177949,
      "retryCount": 23145,
      "timeSpentInError": 2468081212,
      "errorCount": 2
    }
  }
]    
    """


def getReplicationtaskmonitorsWithMultipleTasksDummyResponse():
    return """
[
  {
    "name": "RF_ahm_test_SNWD_PO_glkh1",
    "flowName": "RF_ahm_test",
    "sourceDataset": "SNWD_PO",
    "targetDataset": "TGT_SNWD_PO",
    "priority": "MEDIUM",
    "loadType": "REPLICATE",
    "status": "ERROR",
    "state": "Error",
    "active": false,
    "statusInfo": "connection id not found",
    "numberOfRecordsTransferred": 170,
    "numberOfPartitions": 4,
    "lastRuntimeUpdated": "2021-11-26T03:51:49.7257153",
    "startTime": "2021-11-18T08:35:04.2543123",
    "initialLoadEndTime": "2021-11-18T08:35:29.2876103",
    "partitions": {
      "42010AEE1BB61EEC92894F1B65D1DD23": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-18T08:35:23.4178839",
          "lastAccessedAt": "2021-11-18T08:35:28.1274919",
          "lastRetriedAt": "",
          "lastErrorAt": "",
          "completedAt": "2021-11-18T08:35:28.1274919",
          "sourceBytes": 29920,
          "sourceRecordCount": 170,
          "sourceProcessingTime": 118,
          "transformBytes": 47514,
          "transformRecordCount": 170,
          "targetBytes": 29920,
          "targetRecordCount": 170,
          "targetProcessingTime": 349
        }
      },
      "42010AEE1BB61EEC92894F1B65D1FD23": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-18T08:35:23.4178839",
          "lastAccessedAt": "2021-11-18T08:35:29.2876103",
          "lastRetriedAt": "2021-11-18T08:35:26.3924056",
          "lastErrorAt": "",
          "completedAt": "2021-11-18T08:35:29.2876103",
          "timeSpentRetrying": 164,
          "retryCount": 1
        }
      },
      "42010AEE1BB61EEC92894F1B65D21D23": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-18T08:35:23.4178839",
          "lastAccessedAt": "2021-11-18T08:35:28.5585915",
          "lastRetriedAt": "2021-11-18T08:35:26.3044790",
          "lastErrorAt": "",
          "completedAt": "2021-11-18T08:35:28.5585915",
          "timeSpentRetrying": 135,
          "retryCount": 1
        }
      },
      "42010AEE1BB61EEC92894F8F344B5D5B": {
        "transferMode": "DELTA",
        "status": "Transferring",
        "statusInfo": "Retrying due to data not ready.",
        "retrying": true,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-18T08:35:29.4937336",
          "lastAccessedAt": "2021-11-26T03:51:48.4057569",
          "lastRetriedAt": "2021-11-26T03:51:48.4057569",
          "lastErrorAt": "",
          "completedAt": "",
          "timeSpentRetrying": 259712137,
          "retryCount": 353682
        }
      }
    },
    "initialLoadMetrics": {
      "priorityChangedAt": "2021-11-18T08:34:41.5140502",
      "priorityChangedBy": "cit-admin",
      "firstActivatedAt": "2021-11-18T08:35:04.2543123",
      "lastActivatedAt": "2021-11-18T08:35:04.2543123",
      "lastActivatedBy": "cit-admin",
      "lastSuspendedAt": "2021-11-18T08:34:41.5140502",
      "lastSuspendedBy": "cit-admin",
      "lastRetriedAt": "2021-11-18T08:35:18.2634791",
      "lastErrorAt": "",
      "lastAccessedAt": "2021-11-18T08:35:29.2876103",
      "completedAt": "2021-11-18T08:35:29.2876103",
      "timeSpentActive": 9532,
      "activeCount": 1,
      "timeSpentSuspended": 22740,
      "suspendCount": 1,
      "timeSpentRetrying": 15495,
      "retryCount": 5
    },
    "deltaLoadMetrics": {
      "priorityChangedAt": "",
      "firstActivatedAt": "2021-11-18T08:35:04.2543123",
      "lastActivatedAt": "",
      "lastSuspendedAt": "",
      "lastRetriedAt": "",
      "lastErrorAt": "2021-11-26T03:51:49.7257153",
      "lastAccessedAt": "2021-11-26T03:51:49.7257153",
      "completedAt": "",
      "timeSpentActive": 674180438,
      "timeSpentInError": 446058449,
      "errorCount": 1
    }
  },
  {
    "name": "RF_ahm_test_SNWD_PO_I_mf1e5i",
    "flowName": "RF_ahm_test",
    "sourceDataset": "SNWD_PO_I",
    "targetDataset": "TGT_SNWD_PO_I",
    "priority": "MEDIUM",
    "loadType": "REPLICATE",
    "status": "DELTA_RUNNING",
    "state": "Transferring delta load",
    "active": true,
    "numberOfRecordsTransferred": 1061,
    "numberOfPartitions": 4,
    "lastRuntimeUpdated": "2021-11-18T08:35:33.4674092",
    "startTime": "2021-11-18T08:35:04.2543123",
    "initialLoadEndTime": "2021-11-18T08:35:33.4674092",
    "partitions": {
      "000D3A28DF191EECB1C013C5D1C82913": {
        "transferMode": "INITIAL",
        "status": "Error",
        "statusInfo": "data could not be converted (keys: values for key fields, columns: names of failing columns):\\nkeys=[0000019482] columns=[FieldDats FieldTims]",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2022-04-27T08:04:09.8109002",
          "lastAccessedAt": "2022-04-27T08:04:46.1044655",
          "lastRetriedAt": "",
          "lastErrorAt": "2022-04-27T08:04:46.1044655",
          "completedAt": "",
          "errorCount": 1,
          "sourceBytes": 30277802,
          "sourceRecordCount": 138889,
          "sourceProcessingTime": 1871,
          "transformBytes": 11793106,
          "transformRecordCount": 138889,
          "targetBytes": 30277802,
          "targetRecordCount": 138889,
          "targetProcessingTime": 4119
        }
      },
      "42010AEE1BB61EEC92894F8F344B9D5B": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-18T08:35:30.4073324",
          "lastAccessedAt": "2021-11-18T08:35:31.4811401",
          "lastRetriedAt": "",
          "lastErrorAt": "",
          "completedAt": "2021-11-18T08:35:31.4811401",
          "sourceBytes": 133686,
          "sourceRecordCount": 1061,
          "sourceProcessingTime": 126,
          "transformBytes": 187003,
          "transformRecordCount": 1061,
          "targetBytes": 133686,
          "targetRecordCount": 1061,
          "targetProcessingTime": 300
        }
      },
      "42010AEE1BB61EEC92894F8F344BBD5B": {
        "transferMode": "INITIAL",
        "status": "Complete",
        "retrying": false,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-18T08:35:30.4073324",
          "lastAccessedAt": "2021-11-18T08:35:31.1310155",
          "lastRetriedAt": "",
          "lastErrorAt": "",
          "completedAt": "2021-11-18T08:35:31.1310155"
        }
      },
      "42010AEE1BB61EEC92894FDC3BE7BD5B": {
        "transferMode": "DELTA",
        "status": "Transferring",
        "statusInfo": "Retrying due to data not ready.",
        "retrying": true,
        "partitionMetrics": {
          "firstActivatedAt": "2021-11-18T08:35:33.6469696",
          "lastAccessedAt": "2021-12-01T07:45:39.8531557",
          "lastRetriedAt": "2021-12-01T07:45:38.6457175",
          "lastErrorAt": "",
          "completedAt": "",
          "timeSpentRetrying": 330274071,
          "retryCount": 451560
        }
      }
    },
    "initialLoadMetrics": {
      "priorityChangedAt": "2021-11-18T08:34:42.7171891",
      "priorityChangedBy": "cit-admin",
      "firstActivatedAt": "2021-11-18T08:35:04.2543123",
      "lastActivatedAt": "2021-11-18T08:35:04.2543123",
      "lastActivatedBy": "cit-admin",
      "lastSuspendedAt": "2021-11-18T08:34:42.7171891",
      "lastSuspendedBy": "cit-admin",
      "lastRetriedAt": "2021-11-18T08:35:24.2704317",
      "lastErrorAt": "",
      "lastAccessedAt": "2021-11-18T08:35:33.4674092",
      "completedAt": "2021-11-18T08:35:33.4674092",
      "timeSpentActive": 7355,
      "activeCount": 1,
      "timeSpentSuspended": 21537,
      "suspendCount": 1,
      "timeSpentRetrying": 21849,
      "retryCount": 6
    },
    "deltaLoadMetrics": {
      "priorityChangedAt": "",
      "firstActivatedAt": "2021-11-18T08:35:04.2543123",
      "lastActivatedAt": "",
      "lastSuspendedAt": "",
      "lastRetriedAt": "",
      "lastErrorAt": "",
      "lastAccessedAt": "2021-11-18T08:35:33.4674092",
      "completedAt": "",
      "timeSpentActive": 1120234863
    }
  }
]
    """
