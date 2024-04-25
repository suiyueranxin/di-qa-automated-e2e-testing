import unittest

# Replication, ReplicationLoadtype, ReplicationSpace, ReplicationSpaceProperty, ReplicationSpacePropertyValue_FILE_TYPE, ReplicationSpacePropertyValue_GROUP_DELTA_BY, ReplicationTask, ReplicationTaskFilter, ReplicationTaskFilterOperator, ReplicationTaskFilters
from framework.infrastructure.replications.Replication import *


class testReplication(unittest.TestCase):

    def test_basics(self):
        cut = Replication('someName')
        self.assertEqual('someName', cut.name)
        self.assertEqual('', cut.description)
        self.assertIsNone(cut.version)

    def test_description(self):
        cut = Replication('dummy')
        cut.set_description('description')
        self.assertEqual('description', cut.description)

    def test_createtask(self):
        cut = Replication('withTask')
        cut.set_sourcespace('sourcespace', 'container')
        cut.set_targetspace('targetspace', 'container')
        task = cut.create_task('sourcedataset')
        self.assertIn(task, cut.tasks)
        self.assertTrue(task.name.startswith('withTask_sourcedataset_'))
        self.assertEqual('sourcedataset', task.sourcedataset)
        self.assertEqual('sourcedataset', task.targetdataset)

        self.assertEqual('withTask_sourcespace_src',
                         task._values['sourceSpace'])
        self.assertEqual('withTask_targetspace_tgt',
                         task._values['targetSpace'])

    def test_targetspace(self):
        cut = Replication('withTask')
        cut.set_sourcespace('sourcespace', 'container')
        targetspace = cut.set_targetspace('targetspace', 'container')
        self.assertIsNotNone(targetspace)
        self.assertEqual(targetspace, cut.targetspace)


class testReplicationSpace(unittest.TestCase):

    def test_basics(self):
        cut = ReplicationSpace('someName_S4H_2021_src', 'S4H_2021', '/CDS')
        self.assertEqual('someName_S4H_2021_src', cut.name)
        self.assertEqual('S4H_2021', cut._connectionid)
        self.assertEqual('/CDS', cut._container)
        self.assertIsNone(cut._connectiontype)
        self.assertIsNone(cut._ccmconnectiontype)

    def test_properties(self):
        cut = ReplicationSpace('someName_S4H_2021_src', 'S4H_2021', '/CDS')
        self.assertIsNone(cut._getproperty('PROPERTY_NAME'))
        cut._setproperty('PROPERTY_NAME', 'value')
        self.assertEqual('value', cut._getproperty('PROPERTY_NAME'))
        cut._setproperty('PROPERTY_NAME', 'new_value')
        self.assertEqual('new_value', cut._getproperty('PROPERTY_NAME'))

        self.assertIsNone(cut._getproperty('FOO'))

        cut._clearproperty('PROPERTY_NAME')
        self.assertIsNone(cut._getproperty('PROPERTY_NAME'))

        cut._setproperty(ReplicationSpaceProperty.GROUP_DELTA_BY.value, 'DATE')
        self.assertEqual('DATE', cut._getproperty(
            ReplicationSpaceProperty.GROUP_DELTA_BY.value))


class testReplicationTragetSpace(unittest.TestCase):

    def test_properties(self):
        cut = ReplicationTargetSpace(
            'someName_S4H_2021_src', 'S4H_2021', '/CDS')

        cut.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        self.assertEqual(ReplicationSpaceGroupDeltaBy.DATE.value, cut._getproperty(
            ReplicationSpaceProperty.GROUP_DELTA_BY.value))
        cut.set_file_type(ReplicationSpaceFileType.PARQUET)
        self.assertEqual(ReplicationSpaceFileType.PARQUET.value, cut._getproperty(
            ReplicationSpaceProperty.FILE_TYPE.value))
        cut.set_file_compression(ReplicationSpaceFileCompression.GZIP)
        self.assertEqual(ReplicationSpaceFileCompression.GZIP.value, cut._getproperty(
            ReplicationSpaceProperty.FILE_COMPRESSION.value))
        cut.set_file_delimiter(ReplicationSpaceFileDelimiter.COMMA)
        self.assertEqual(ReplicationSpaceFileDelimiter.COMMA.value, cut._getproperty(
            ReplicationSpaceProperty.FILE_DELIMITER.value))
        cut.set_file_header(True)
        self.assertEqual('true', cut._getproperty(
            ReplicationSpaceProperty.FILE_HEADER.value))
        cut.set_file_header(False)
        self.assertEqual('false', cut._getproperty(
            ReplicationSpaceProperty.FILE_HEADER.value))


class testReplicationSpaceProperties(unittest.TestCase):

    def test_propertynames(self):
        cut = ReplicationSpaceProperty.GROUP_DELTA_BY
        self.assertEqual('groupDeltaFilesBy', cut.value)
        cut = ReplicationSpaceProperty.FILE_TYPE
        self.assertEqual('format', cut.value)
        cut = ReplicationSpaceProperty.FILE_COMPRESSION
        self.assertEqual('compression', cut.value)
        cut = ReplicationSpaceProperty.FILE_HEADER
        self.assertEqual('isHeaderIncluded', cut.value)
        cut = ReplicationSpaceProperty.FILE_DELIMITER
        self.assertEqual('columnDelimiter', cut.value)

    def test_propertyvalues(self):
        cut = ReplicationSpaceGroupDeltaBy.DATE
        self.assertEqual('DATE', cut.value)
        cut = ReplicationSpaceGroupDeltaBy.HOUR
        self.assertEqual('HOUR', cut.value)
        cut = ReplicationSpaceGroupDeltaBy.NONE
        self.assertEqual('NONE', cut.value)

        cut = ReplicationSpaceFileType.PARQUET
        self.assertEqual('PARQUET', cut.value)
        cut = ReplicationSpaceFileType.CSV
        self.assertEqual('CSV', cut.value)

        cut = ReplicationSpaceFileCompression.NONE
        self.assertEqual('NONE', cut.value)
        cut = ReplicationSpaceFileCompression.GZIP
        self.assertEqual('GZIP', cut.value)
        cut = ReplicationSpaceFileCompression.SNAPPY
        self.assertEqual('SNAPPY', cut.value)

        cut = ReplicationSpaceFileDelimiter.COMMA
        self.assertEqual('COMMA', cut.value)
        cut = ReplicationSpaceFileDelimiter.COLON
        self.assertEqual('COLON', cut.value)
        cut = ReplicationSpaceFileDelimiter.PIPE
        self.assertEqual('PIPE', cut.value)
        cut = ReplicationSpaceFileDelimiter.SEMICOLON
        self.assertEqual('SEMICOLON', cut.value)
        cut = ReplicationSpaceFileDelimiter.TAB
        self.assertEqual('TAB', cut.value)


class testReplicationTask(unittest.TestCase):

    def test_skeleton(self):
        cut = ReplicationTask('replication_sourcedataset_uid')
        self.assertEqual('replication_sourcedataset_uid', cut.name)
        cut.set_sourcedataset('sourcedataset')
        self.assertEqual('sourcedataset', cut.sourcedataset)
        cut.set_targetdataset('targetdataset')
        self.assertEqual('targetdataset', cut.targetdataset)
        self.assertEqual(ReplicationLoadtype.INITIAL, cut.loadtype)
        self.assertEqual([], cut.mappings)
        # self.assertEqual([], cut.filters)
        self.assertEqual('', cut.description)

        cut.set_description('just a test')
        self.assertEqual('just a test', cut.description)

        cut.set_sourcespace('sourcespaceId')
        self.assertEqual('sourcespaceId', cut._values['sourceSpace'])

        cut.set_targetspace('targetspaceId')
        self.assertEqual('targetspaceId', cut._values['targetSpace'])

        cut.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)
        self.assertEqual('REPLICATE', cut._values['loadType'])
        self.assertEqual(ReplicationLoadtype.INITIAL_AND_DELTA, cut.loadtype)

        cut.set_truncate(True)
        self.assertTrue(cut._values['truncate'])
        self.assertTrue(cut.truncate)

    def test_filters(self):
        cut = ReplicationTask('replication_sourcedataset_uid')
        self.assertIsNotNone(cut.filters)
        self.assertEqual(0, len(cut.filters.filters))


class testReplicationLoadtype(unittest.TestCase):
    def test_enumvalues(self):
        cut = ReplicationLoadtype.INITIAL
        self.assertEqual('INITIAL', cut.value)
        cut = ReplicationLoadtype.INITIAL_AND_DELTA
        self.assertEqual('REPLICATE', cut.value)


class testReplicationTaskFilters(unittest.TestCase):
    def test_basics(self):
        values = []
        cut = ReplicationTaskFilters(values)
        filters = cut.filters

        cut.add_filter('COLNAME', ReplicationTaskFilterOperator.EQUALS, '123')
        self.assertEqual(1, len(filters))
        filter = filters[0]
        self.assertEqual('COLNAME', filter.name)
        self.assertEqual(ReplicationTaskFilterOperator.EQUALS,
                         filter.operator)
        self.assertEqual('123', filter.operand)
        self.assertEqual(1, len(values))

        cut.add_filter('ANOTHER_COLNAME',
                       ReplicationTaskFilterOperator.EQUALS, '234')
        self.assertEqual(2, len(filters))
        filter = filters[1]
        self.assertEqual('ANOTHER_COLNAME', filter.name)
        self.assertEqual(ReplicationTaskFilterOperator.EQUALS,
                         filter.operator)
        self.assertEqual('234', filter.operand)
        self.assertEqual(2, len(values))

        cut.add_filter('ANOTHER_COLNAME',
                       ReplicationTaskFilterOperator.EQUALS, '345')
        self.assertEqual(3, len(filters))
        filter = filters[2]
        self.assertEqual('ANOTHER_COLNAME', filter.name)
        self.assertEqual(ReplicationTaskFilterOperator.EQUALS,
                         filter.operator)
        self.assertEqual('345', filter.operand)
        self.assertEqual(2, len(values))


class testReplicationTaskFilterOperator(unittest.TestCase):
    def test_enumvalues(self):
        cut = ReplicationTaskFilterOperator.EQUALS
        self.assertEqual('=', cut.value)


class testReplicationTaskFilter(unittest.TestCase):
    def test_basics(self):
        cut = ReplicationTaskFilter(
            'COLNAME', ReplicationTaskFilterOperator.EQUALS, '123')
        self.assertEqual('COLNAME', cut.name)
        self.assertEqual(ReplicationTaskFilterOperator.EQUALS,
                         cut.operator)
        self.assertEqual('123', cut.operand)
        self.assertEqual(None, cut.second_operand)


if __name__ == '__main__':
    unittest.main()
