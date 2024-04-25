from __future__ import annotations
from typing import TYPE_CHECKING

from enum import Enum
import string
import random


class Replication:
    def __init__(self, name) -> None:
        self._name = name
        self._description = ''
        self._version = None
        self._oneSourceOneTargetTasks = []

    @property
    def name(self) -> str:
        return self._name

    def set_description(self, description: str):
        self._description = description

    @property
    def description(self) -> str:
        return self._description

    @property
    def version(self) -> str:
        return self._version

    @property
    def sourcespace(self) -> ReplicationSpace:
        return self._sourcespace

    @property
    def targetspace(self) -> ReplicationTargetSpace:
        return self._targetspace

    def set_sourcespace(self, connection_id: str, container: str):
        """ Sets the connection id and container of the source space.

        Parameters
        ----------
        connection_id: str
            The id of the connection to the source system. This will be used to
            retrieve the connection information from the connection management.

        container: str
            The container to be used within the source system.
        """

        self._sourcespace = ReplicationSpace(
            self._name + '_' + connection_id + '_src', connection_id, container)

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

        container: str
            The container to be used within the target system.
        """

        self._targetspace = ReplicationTargetSpace(
            self._name + '_' + connection_id + '_tgt', connection_id, container)
        return self._targetspace

    def create_task(self, source_dataset: str):
        """ Creates a new task for the given source dataset and returns it.

        Parameters
        ----------
        source_dataset: str
            The name of the source dataset for which the taks will be created.
        """

        characters = string.ascii_lowercase + string.digits
        uid = ''.join(random.choice(characters) for i in range(6))
        name = self._name + '_' + source_dataset + '_' + uid
        task = ReplicationTask(name)
        self._oneSourceOneTargetTasks.append(task)
        task.set_sourcedataset(source_dataset)
        task.set_sourcespace(self._sourcespace.name)
        task.set_targetdataset(source_dataset)
        task.set_targetspace(self._targetspace.name)

        return task

    @property
    def tasks(self) -> list[ReplicationTask]:
        return self._oneSourceOneTargetTasks


class ReplicationSpace:
    """A class that acts as container for information about source and target spaces."""

    def __init__(self, name: str, connection_id: str, container: str) -> None:
        self._name = name
        self._connectionid = connection_id
        self._container = container
        self._connectiontype = None
        self._ccmconnectiontype = None
        self._datasetProperties = None

    @property
    def name(self) -> str:
        return self._name

    def _getproperty(self, name: str):
        if self._datasetProperties is None:
            return None

        return self._datasetProperties.get(name, None)

    def _setproperty(self, name: str, value: str):
        if self._datasetProperties is None:
            self._datasetProperties = {}

        self._datasetProperties[name] = value

    def _clearproperty(self, name: str):
        if self._datasetProperties is None:
            return

        del self._datasetProperties[name]


class ReplicationTargetSpace(ReplicationSpace):
    """A class that acts as Container for information about target spaces"""

    def set_file_type(self, filetype: ReplicationSpaceFileType):
        """ Sets the "File Type" property of the space.

        Parameters
        ----------
        filetype : ReplicationSpaceFileType
            A value from the ReplicationSpaceFileType enum.        
        """

        self._setproperty(
            ReplicationSpaceProperty.FILE_TYPE.value, filetype.value)

    def set_groupdeltaby(self, group_delta_by: ReplicationSpaceGroupDeltaBy):
        """ Sets the "Group Delta By" property of the space.

        Parameters
        ----------
        group_delta_by : ReplicationSpaceGroupDeltaBy
            A value from the ReplicationSpaceGroupDeltaBy enum.        
        """

        self._setproperty(
            ReplicationSpaceProperty.GROUP_DELTA_BY.value, group_delta_by.value)

    def set_file_compression(self, compression: ReplicationSpaceFileCompression):
        """ Sets the "Compression" property of the space.

        This is only supported if the "File Type" has been set to "PARQUET" via
        the `setfiletype` method.

        Parameters
        ----------
        compression : ReplicationSpaceFileCompression
            A value from the ReplicationSpaceFileCompression enum.        
        """

        self._setproperty(
            ReplicationSpaceProperty.FILE_COMPRESSION.value, compression.value)

    def set_file_delimiter(self, delimiter: ReplicationSpaceFileDelimiter):
        """ Sets the "File Delimiter" property of the space.

        This is only supported if the "File Type" has been set to "CSV" via
        the `setfiletype` method.

        Parameters
        ----------
        delimiter : ReplicationSpaceFileDelimiter
            A value from the ReplicationSpaceFileDelimiter enum.        
        """

        self._setproperty(
            ReplicationSpaceProperty.FILE_DELIMITER.value, delimiter.value)

    def set_file_header(self, include: bool):
        """ Sets the "File Header" property of the space.

        This is only supported if the "File Type" has been set to "CSV" via
        the `setfiletype` method.

        Parameters
        ----------
        include : bool        
        """

        stringvalue = 'false'
        if include:
            stringvalue = 'true'
        self._setproperty(
            ReplicationSpaceProperty.FILE_HEADER.value, stringvalue)


class ReplicationSpaceProperty(Enum):
    GROUP_DELTA_BY = 'groupDeltaFilesBy'
    FILE_TYPE = 'format'
    FILE_COMPRESSION = 'compression'
    FILE_HEADER = 'isHeaderIncluded'
    FILE_DELIMITER = 'columnDelimiter'


class ReplicationSpaceGroupDeltaBy(Enum):
    DATE = 'DATE'
    HOUR = 'HOUR'
    NONE = 'NONE'


class ReplicationSpaceFileType(Enum):
    PARQUET = 'PARQUET'
    CSV = 'CSV'


class ReplicationSpaceFileCompression(Enum):
    NONE = 'NONE'
    GZIP = 'GZIP'
    SNAPPY = 'SNAPPY'


class ReplicationSpaceFileDelimiter(Enum):
    COMMA = 'COMMA'
    COLON = 'COLON'
    PIPE = 'PIPE'
    SEMICOLON = 'SEMICOLON'
    TAB = 'TAB'


class ReplicationTask:
    """A class that represents a replication task."""

    def __init__(self, name: str) -> None:
        self._values = {}
        self._values['name'] = name
        self._values['description'] = ''
        self._values['mappings'] = []
        self._values['filter'] = []
        self._values['loadType'] = 'INITIAL'
        self._values['truncate'] = False
        self._filters = ReplicationTaskFilters(self._values['filter'])

    @property
    def name(self) -> str:
        return self._values['name']

    def set_sourcedataset(self, path: str):
        """Sets the path to the source dataset.

        The source dataset can be a database table/view or a file/folder in a
        file system-like storage. The path is relative to source container that
        is specified for the replication.
        """

        self._values['sourceDataset'] = path

    @property
    def sourcedataset(self) -> str:
        return self._values['sourceDataset']

    def set_targetdataset(self, path: str):
        """Sets the path to the target dataset.

        The target dataset can be a database table/view or a file/folder in a
        file system-like storage.  The path is relative to target container that
        is specified for the replication.       
        """
        self._values['targetDataset'] = path

    @property
    def targetdataset(self) -> str:
        return self._values['targetDataset']

    def set_loadtype(self, loadtype: ReplicationLoadtype):
        """ Sets the "Load Type" property of the task.

        Parameters
        ----------
        loadtype : ReplicationLoadtype
            A value from the ReplicationLoadtype enum.        
        """

        self._values['loadType'] = loadtype.value

    @property
    def loadtype(self) -> ReplicationLoadtype:
        return ReplicationLoadtype(self._values.get('loadType'))

    def set_truncate(self, truncate: bool) -> None:
        """Sets whether the Truncate option is active which clears the content 
        of the target before the task runs."""
        self._values['truncate'] = truncate

    @property
    def truncate(self) -> bool:
        """Gets whether the Truncate option is active which clears the content 
        of the target before the task runs."""
        return self._values['truncate']

    @property
    def mappings(self):
        return self._values['mappings']

    @property
    def filters(self) -> ReplicationTaskFilters:
        return self._filters

    def set_description(self, description: str):
        self._values['description'] = description

    @property
    def description(self) -> str:
        return self._values['description']

    def set_sourcespace(self, sourcespace):
        self._values['sourceSpace'] = sourcespace

    def set_targetspace(self, targetspace):
        self._values['targetSpace'] = targetspace


class ReplicationLoadtype(Enum):
    INITIAL = 'INITIAL'
    INITIAL_AND_DELTA = 'REPLICATE'


class ReplicationTaskFilters():
    def __init__(self, values) -> None:
        self._filters = []
        self._values = values
        self._filterdict = {}

    def add_filter(self, fieldname: str, operator: ReplicationTaskFilterOperator, operand: str, second_operand: str = None) -> None:
        """Adds a new filter for the given field/column based on the given operator and operand(s)

        Parameters
        ----------
        fieldname : str
            The name of the field/column for which the the filter is defined.
        operator : ReplicationTaskFilterOperator
            The operator for the filter. Potential operators are provided by the
            ReplicationTaskFilterOperator enum. Some operators require the 
            `secondoperand` parameter to be provided.
        operand : str
            The operand for the operator
        second_operand : str, optional
            The second operand. **Only to be provided if the operator requires a
            second operand.** Please see the documentation of .. class:: ReplicationTaskFilterOperator
            for the list of operators that require a second operand.
        """

        taskfilter = ReplicationTaskFilter(
            fieldname, operator, operand, second_operand)

        self._filters.append(taskfilter)

        elements = None

        for entry in self._values:
            if entry['name'] == fieldname:
                elements = entry['elements']
                break

        if elements is None:
            elements = []
            entry = {'name': fieldname, 'elements': elements}
            self._values.append(entry)

        elements.append({'comparison': operator.value, 'low': operand})

    @property
    def filters(self) -> list[ReplicationTaskFilter]:
        return self._filters


class ReplicationTaskFilterOperator(Enum):
    """An enumeration of the operators for task filters.

    Most operators only require a single operand. However the following
    operators require a second operand:

    BETWEEN
    """
    EQUALS = '='


class ReplicationTaskFilter():
    def __init__(self, fieldname: str, operator: ReplicationTaskFilterOperator, operand: str, second_operand: str = None) -> None:
        self._name = fieldname
        self._operator = operator
        self._operand = operand
        self._second_operand = second_operand

    @property
    def name(self) -> str:
        return self._name

    @property
    def operator(self) -> ReplicationTaskFilterOperator:
        return self._operator

    @property
    def operand(self) -> str:
        return self._operand

    @property
    def second_operand(self) -> str:
        return self._second_operand
