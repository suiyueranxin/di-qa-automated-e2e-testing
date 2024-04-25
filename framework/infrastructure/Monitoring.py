from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List

import json
from enum import Enum

from isodate import parse_datetime

if TYPE_CHECKING:
    from framework.infrastructure.Cluster import Cluster


class Monitoring:

    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster
        self._replications = ReplicationsMonitoring(self)

    @property
    def cluster(self) -> Cluster:
        return self._cluster

    @property
    def replications(self) -> ReplicationsMonitoring:
        return self._replications


class ReplicationsMonitoring:
    """
    For documentation of the used service see:
    https://github.wdf.sap.corp/pages/bdh/rms/api/dt/spec.html#/replicationFlowMonitor
    """

    def __init__(self, monitoring: Monitoring) -> None:
        self._monitoring = monitoring

    def get_monitors(self) -> List[ReplicationMonitor]:
        response = self._monitoring.cluster.apiget(
            '/app/rms/api/dt/v1/replicationflowMonitors')
        monitors: List[ReplicationMonitor] = []
        responsedata = json.loads(response.text)
        for entry in responsedata:
            monitors.append(ReplicationMonitor(entry))
        return monitors

    def get_monitor(self, name) -> ReplicationMonitor:
        response = self._monitoring.cluster.apiget(
            f'/app/rms/api/dt/v1/replicationflowMonitors?name={name}')
        responsedata = json.loads(response.text)

        monitor = ReplicationMonitor(responsedata[0])
        if name == monitor.name:
            return monitor

        return None

    def get_taskmonitors(self, name) -> List[Replicationtaskmonitor]:
        response = self._monitoring.cluster.apiget(
            f'/app/rms/api/dt/v1/replicationflows/{name}/taskMonitors')
        monitors: List[Replicationtaskmonitor] = []
        responsedata = json.loads(response.text)
        for entry in responsedata:
            monitors.append(Replicationtaskmonitor(entry))
        return monitors


class ReplicationMonitor:

    def __init__(self, values: Dict) -> None:
        self._values: Dict = values

    @property
    def name(self) -> str:
        return self._values['name']

    @property
    def status(self) -> str:
        return self._values.get('status', None)

    @property
    def taskmetrics(self) -> ReplicationMonitorTaskMetrics:
        if 'taskMetrics' in self._values:
            return ReplicationMonitorTaskMetrics(self._values['taskMetrics'])

        return None

    def __str__(self) -> str:
        name = self.name
        status = self.status
        taskmetrics = str(self.taskmetrics)
        return f'{name}:\t{status} - {taskmetrics}'


class ReplicationMonitorTaskMetrics:

    def __init__(self, taskMetrics) -> None:
        self._values = taskMetrics

    @property
    def total(self) -> int:
        return self._values.get('total', 0)

    @property
    def completed(self) -> int:
        return self._values.get('completed', 0)

    @property
    def error(self) -> int:
        return self._values.get('error', 0)

    @property
    def initial_completed(self) -> int:
        return self._values.get('initialCompleted', 0)

    @property
    def created(self) -> int:
        return self._values.get('created', 0)

    def __str__(self) -> str:
        total = self.total
        error = self.error
        completed = self.completed
        initial_completed = self.initial_completed
        created = self.created
        return f'Total:\t{total}, Error:\t{error}, Completed:\t{completed}, Initial completed:\t{initial_completed}, Created:\t{created}'


class Replicationtaskmonitor():
    def __init__(self, data) -> None:
        self._data = data

    @property
    def name(self) -> str:
        """
        Replication task Name
        """
        return self._data.get('name')

    @property
    def status(self) -> ReplicationtaskmonitorStatus:
        status = self._data.get('status')
        return ReplicationtaskmonitorStatus(status)

    @property
    def statusinfo(self) -> str:
        return self._data.get('statusInfo', '')

    @property
    def partitions(self) -> list[ReplicationtaskmonitorPartition]:
        partitions = []
        partitions_data = self._data.get('partitions')
        if partitions_data is None:
            return partitions
        for key, value in partitions_data.items():
            partitions.append(
                ReplicationtaskmonitorPartition(key, value))

        return partitions

    @property
    def numberOfRecordsTransferred(self) -> int:
        """
        Number of records transferred to the target dataset
        """
        return self._data.get('numberOfRecordsTransferred')

    @property
    def startTime(self) -> datetime:
        """
        UTC time of when task starts initial load
        """
        time_string = self._data.get('startTime')
        time_value = parse_datetime(time_string)
        return time_value

    @property
    def lastRuntimeUpdated(self) -> datetime:
        """
        UTC time of the last time the task had its runtime data updated. This 
        could be updated when data transferring or configuration properties are 
        changed. Configuration change properties are any task's priority and 
        task's active state change. This property is useful to understand if 
        some activities related to runtime of task are happening or task is 
        sitting idle doing nothing.
        """
        time_string = self._data.get('lastRuntimeUpdated')
        time_value = parse_datetime(time_string)
        return time_value

    @property
    def initialLoadEndTime(self) -> datetime:
        """
        UTC time of when task completed initial load
        """
        return _get_datetime_value(self._data, 'initialLoadEndTime')


def _get_datetime_value(dictionary: dict, key: str) -> datetime:
    time_string = dictionary.get(key)
    if time_string is not None:
        time_value = parse_datetime(time_string)
        return time_value


class ReplicationtaskmonitorPartition():
    def __init__(self, partition_id: str, data: dict) -> None:
        self._id = partition_id
        self._data = data
        self._metrics = self._data['partitionMetrics']

    @property
    def id(self) -> str:
        return self._id

    @property
    def status(self) -> str:
        """
        Partition status
        """
        return self._data.get('status')

    @property
    def statusInfo(self) -> str:
        """
        Partition related status info. It may be error text when status = Error
        """
        return self._data.get('statusInfo')

    @property
    def firstActivatedAt(self) -> datetime:
        return _get_datetime_value(self._metrics, 'firstActivatedAt')

    @property
    def lastAccessedAt(self) -> datetime:
        """
        UTC time of when the partition was last updated
        """
        return _get_datetime_value(self._metrics, 'lastAccessedAt')

    @property
    def lastRetriedAt(self) -> datetime:
        """
        UTC time of the last time partition was set to retry
        """
        return _get_datetime_value(self._metrics, 'lastRetriedAt')

    @property
    def lastErrorAt(self) -> datetime:
        """
        UTC time of the last time partition encountered a non-retryable error
        """
        return _get_datetime_value(self._metrics, 'lastErrorAt')

    @property
    def completedAt(self) -> datetime:
        """
        UTC time of the time when partition completed (NO_MORE_DATA received)
        """
        return _get_datetime_value(self._metrics, 'completedAt')

    @property
    def sourceBytes(self) -> int:
        """
        Total bytes processed by reader
        """
        return self._metrics.get('sourceBytes', 0)

    @property
    def sourceRecordCount(self) -> int:
        """
        Total records processed by reader
        """
        return self._metrics.get('sourceRecordCount', 0)

    @property
    def sourceProcessingTime(self) -> int:
        """
        Time in milliseconds that reader spent reading data
        """
        return self._metrics.get('sourceProcessingTime', 0)

    @property
    def transformBytes(self) -> int:
        """
        Total bytes processed by transform
        """
        return self._metrics.get('transformBytes', 0)

    @property
    def transformRecordCount(self) -> int:
        """
        Total records processed by transform
        """
        return self._metrics.get('transformRecordCount', 0)

    @property
    def targetBytes(self) -> int:
        """
        Total bytes processed by writer
        """
        return self._metrics.get('targetBytes', 0)

    @property
    def targetRecordCount(self) -> int:
        """
        Total records processed by writer
        """
        return self._metrics.get('targetRecordCount', 0)

    @property
    def targetProcessingTime(self) -> int:
        """
        Time in milliseconds that writer spent writing data
        """
        return self._metrics.get('targetProcessingTime', 0)


class ReplicationtaskmonitorStatus(Enum):
    CREATED = 'CREATED'
    INITIAL_RUNNING = 'INITIAL_RUNNING'
    DELTA_RUNNING = 'DELTA_RUNNING'
    SUSPENDING = 'SUSPENDING'
    SUSPENDED = 'SUSPENDED'
    RETRYING = 'RETRYING'
    COMPLETED = 'COMPLETED'
    ERROR = 'ERROR'
