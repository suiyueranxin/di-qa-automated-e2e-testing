from __future__ import annotations
from typing import TYPE_CHECKING

import json
from time import sleep
from enum import Enum

from framework.infrastructure.Modeler import ModelerReplicationJson
from framework.infrastructure.replications import Replication
if TYPE_CHECKING:
    from framework.infrastructure.Cluster import Cluster


# Designtime API: https://github.wdf.sap.corp/pages/bdh/rms/api/dt/spec.html
# Runtime API: https://github.wdf.sap.corp/pages/bdh/rms/api/rt/spec.html


class Rms:
    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster

    @property
    def cluster(self) -> Cluster:
        return self._cluster

    def createreplicationflow(self, replication: Replication) -> Replicationflow:
        data = ModelerReplicationJson.serialize(replication)
        response = self._cluster.apipost(
            '/app/rms/api/dt/v1/replicationflows', data)

        if response.status_code >= 400:
            return None

        responsedata = json.loads(response.text)
        replicationflow = Replicationflow(replication.name, self)
        replicationflow._changerequeststatusurl = responsedata['url']
        return replicationflow

    def deletereplicationflow(self, replication) -> Replicationflow:
        path = '/app/rms/api/dt/v1/replicationflows/' + replication.name
        response = self._cluster.apidelete(path)

        if response.status_code >= 400:
            return None

        responsedata = json.loads(response.text)
        replicationflow = Replicationflow(replication.name, self)
        replicationflow._changerequeststatusurl = responsedata['url']
        return replicationflow


class Replicationflow:
    def __init__(self, name: str, rms: Rms) -> None:
        self._name = name
        self._rms = rms
        self._changerequeststatusurl = None

        self._waittime = 0.5
        #self._waittries = 60

    def getchangerequeststatus(self) -> Changerequeststatus:
        if self._changerequeststatusurl is None:
            return None

        response = self._rms.cluster.apiget(
            '/app/rms' + self._changerequeststatusurl)
        responsedata = json.loads(response.text)
        return Changerequeststatus(responsedata)

    def runorresume(self):
        response = self._rms.cluster.apiput(
            '/app/rms/api/dt/v1/replicationflows/' + self._name + '?requestType=RUN_OR_RESUME_ALL_INACTIVE_TASKS', '')
        return response

    def waitwhile(self, wait_status) -> Changerequeststatus:
        changerequeststatus = None
        busy = True
        while busy:
            changerequeststatus = self.getchangerequeststatus()
            status = changerequeststatus.getstatus()
            busy = status in wait_status
            sleep(self._waittime)

        return changerequeststatus

    def waitwhilebusy(self) -> Changerequeststatus:
        return self.waitwhile([ChangerequeststatusStatus.VALIDATING, ChangerequeststatusStatus.PROCESSING])


class Changerequeststatus:

    def __init__(self, data) -> None:
        self._data = data
        self._status = ChangerequeststatusStatus(self._data['status'])

    def getstatus(self) -> ChangerequeststatusStatus:
        return self._status

    def __str__(self) -> str:
        return json.dumps(self._data, indent=4)


class ChangerequeststatusStatus(Enum):

    PROCESSING = 'PROCESSING'
    VALIDATING = 'VALIDATING'
    COMPLETED = 'COMPLETED'
    ERROR = 'ERROR'
    PENDING = 'PENDING'
