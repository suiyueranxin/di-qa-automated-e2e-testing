from __future__ import annotations
from typing import TYPE_CHECKING

import json

from framework.infrastructure.replications.Replication import Replication, ReplicationSpace, ReplicationTargetSpace, ReplicationTask
from framework.infrastructure.graphs.Graph import Graph

if TYPE_CHECKING:
    from framework.infrastructure.Rms import Replicationflow
    from framework.infrastructure.Cluster import Cluster


class Modeler:
    def __init__(self, cluster: Cluster):
        self._replications = ReplicationsModeler(cluster)
        self._graph = Graph(cluster)

    @property
    def replications(self):
        """The Replications in the Modeler"""

        return self._replications

    @property
    def graph(self):
        """The Graphs in the Modeler"""
        return self._graph


class ReplicationsModeler:
    def __init__(self, cluster: Cluster):
        self._cluster = cluster

    @property
    def cluster(self) -> Cluster:
        return self._cluster

    def create_replication(self, name: str) -> ModelerReplication:
        """Creates a new replication with the given name.

        The replication with the given name is created and automatically saved.
        """

        # validate name and check whether it's already in use
        replication = ModelerReplication(name, self)
        replication._version = 'ONE_SOURCE_ONE_TARGET'
        replication.save()

        return replication

    def delete_replication(self, name: str) -> None:
        """Deletes a replication with the given name.

        The replication with the given name is deleted from user repository
        """
        path = 'files/rms/' + name + '.replication'
        self._cluster.repository.remove('user', path)

    def open_replication(self, name: str) -> ModelerReplication:
        """Opens the replication with the given name.

        Opens the replication with the given name by loading it from the 
        repository.
        """

        path = 'files/rms/' + name + '.replication'
        response = self._cluster.repository.read('user', path)
        replication = ModelerReplicationJson.deserialize(response.text, self)
        return replication


class ModelerReplication(Replication):
    def __init__(self, name: str, modeler: ReplicationsModeler) -> None:
        super().__init__(name)
        self._modeler = modeler

    def set_sourcespace(self, connection_id: str, container: str) -> None:
        """ Sets the connection id and container of the source space.

        Parameters
        ----------
        connection_id: str
            The id of the connection to the source system. This will be used to
            retrieve the connection information from the connection management.
            If no connection with a matching id exists, a ValueError will be 
            raised.

        container: str
            The container to be used within the source system.

        Raises
        ------
        ValueError
            If the connection management could not find a connection for the
            given connection id.
        """

        super().set_sourcespace(connection_id, container)
        connectionmanagement = self._modeler.cluster.connectionmanagement
        connection = connectionmanagement.getconnection(connection_id)
        if connection is None:
            raise ValueError()

        self._sourcespace._connectiontype = connection.type
        self._sourcespace._ccmconnectiontype = connection.ccmTypeId

    def set_targetspace(self, connection_id: str, container: str) -> ReplicationTargetSpace:
        """ Sets the connection id and container of the target space and returns it.

        The target space instance that gets created based on the given parameters 
        is returned so that addtional properties can be set (e.g. file type and
        delimiters for file system like targets).

        Parameters
        ----------
        connection_id: str
            The id of the connection to the target system. This will be used to
            retrieve the connection information from the connection management.
            If no connection with a matching id exists, a ValueError will be 
            raised.

        container: str
            The container to be used within the target system.

        Raises
        ------
        ValueError
            If the connection management could not find a connection for the
            given connection id.
        """

        super().set_targetspace(connection_id, container)
        connectionmanagement = self._modeler.cluster.connectionmanagement
        connection = connectionmanagement.getconnection(connection_id)
        if connection is None:
            raise ValueError()

        self._targetspace._connectiontype = connection.type
        self._targetspace._ccmconnectiontype = connection.ccmTypeId
        return self._targetspace

    def save(self) -> None:
        """Saves the replication to the cluster's repository."""

        repository = self._modeler.cluster.repository
        path = 'files/rms/' + self.name + '.replication'
        payload = ModelerReplicationJson.serialize(self)
        repository.write('user', path, payload)

    def deploy(self) -> Replicationflow:
        """Deploys the replication and return the corresponding replication flow.

        A replication is basically only the building plan for a replication flow
        which then can be run. This replication flow is generated during the 
        deployment of the replication and the object that represents the 
        replication flow is returned as result. If deployment fails None is 
        returned.

        Returns
        -------
        Replicationflow
            The replication flow that is the result of the deployment. If the
            deployment fails None is returned.
        """

        rms = self._modeler.cluster.rms
        return rms.createreplicationflow(self)

    def run(self):
        pass

    def suspend(self):
        pass

    def undeploy(self) -> Replicationflow:
        """Undeploys the replication and return the corresponding replication flow.

        Returns
        -------
        Replicationflow
            The replication flow that is the result of the undeployment. If the
            undeployment fails None is returned.
        """
        rms = self._modeler.cluster.rms
        return rms.deletereplicationflow(self)


class ModelerReplicationJson:
    """This class provides a static method `serialize` to serialize replications to JSON format."""

    @staticmethod
    def serialize(replication: Replication) -> str:
        """Serializes the given replication to JSON and returns the JSON as string.

        Returns
        -------
        str
            JSON representation of the given replication object.
        """

        jsondata = {}
        jsondata['name'] = replication.name
        jsondata['description'] = replication.description
        jsondata['version'] = replication.version

        sourcespace = ModelerReplicationJson._get_replicationspace_values(
            replication, False)

        sourcespaces = []
        sourcespaces.append(sourcespace)
        jsondata['sourceSpaces'] = sourcespaces

        targetspace = ModelerReplicationJson._get_replicationspace_values(
            replication, True)

        targetspaces = []
        targetspaces.append(targetspace)
        jsondata['targetSpaces'] = targetspaces

        oneSourceOneTargetTasks = []

        for task in replication.tasks:
            oneSourceOneTargetTasks.append(task._values)

        jsondata['oneSourceOneTargetTasks'] = oneSourceOneTargetTasks

        return json.dumps(jsondata)

    @staticmethod
    def _get_replicationspace_values(replication: Replication, is_target: bool):
        space = None
        suffix = ''

        if is_target:
            if hasattr(replication, '_targetspace'):
                space = replication.targetspace
                suffix = 'tgt'
        else:
            if hasattr(replication, '_sourcespace'):
                space = replication.sourcespace
                suffix = 'src'

        values = {}
        if space is not None:
            values['name'] = f'{replication.name}_{space._connectionid}_{suffix}'
            values['connectionId'] = space._connectionid
            values['connectionType'] = space._connectiontype
            values['technicalName'] = space._connectionid
            values['ccmConnectionId'] = space._connectionid
            values['ccmConnectionType'] = space._ccmconnectiontype
            values['container'] = space._container
            if space._datasetProperties is not None:
                values['datasetProperties'] = space._datasetProperties
        else:
            values['name'] = ''
            values['connectionId'] = ''
            values['connectionType'] = ''
            values['technicalName'] = ''
            values['ccmConnectionId'] = ''
            values['ccmConnectionType'] = ''
            values['container'] = ''
        return values

    @staticmethod
    def deserialize(jsoncontent: str, replications_modeler: ReplicationsModeler = None) -> Replication:
        values = json.loads(jsoncontent)

        replication = None
        if replications_modeler is None:
            replication = Replication(values['name'])
        else:
            replication = ModelerReplication(
                values['name'], replications_modeler)

        replication.set_description(values['description'])
        replication._version = values['version']

        source_space_values = values['sourceSpaces'][0]
        sourcespace = ModelerReplicationJson._set_replicationspace_value(
            source_space_values)
        replication._sourcespace = sourcespace

        target_space_values = values['targetSpaces'][0]
        targetspace = ModelerReplicationJson._set_replicationspace_value(
            target_space_values)
        replication._targetspace = targetspace

        tasks_values = values['oneSourceOneTargetTasks']
        for task_value in tasks_values:
            replication_task = ReplicationTask(task_value['name'])
            replication_task._values = task_value
            replication.tasks.append(replication_task)

        return replication

    @staticmethod
    def _set_replicationspace_value(space_values):
        name = space_values['name']
        connection_id = space_values['connectionId']
        container = space_values['container']
        space = ReplicationTargetSpace(
            name, connection_id, container)
        space._connectiontype = space_values['connectionType']
        space._ccmconnectiontype = space_values['ccmConnectionType']
        dataset_properties = space_values.get('datasetProperties', None)
        if dataset_properties is not None:
            space._datasetProperties = space_values['datasetProperties']
        return space
