from __future__ import annotations
from typing import TYPE_CHECKING

from pyrfc import Connection

from framework.validation.abap.AbapClient import AbapClient, AbapConnectionData


class CitAbapClient(AbapClient):
    """A class for accessing CIT content on ABAP systems. Please see 
    https://wiki.wdf.sap.corp/wiki/display/Odin/Test+Data+for+CIT
    for details on the CIT test data.

    The access to the CIT test data is done via the DHE2E_CIT_RFC_DATA_ACCESS 
    RFC function module."""

    def __init__(self) -> None:
        self._cit_prefix = 'DHE2E'

    @staticmethod
    def connect_to(connection_data: AbapConnectionData = None, **kwargs) -> CitAbapClient:
        abapclient = CitAbapClient()

        if connection_data is None:
            abapclient._connection = Connection(
                user=kwargs['user'], passwd=kwargs['passwd'], ashost=kwargs['ashost'], sysnr=kwargs['sysnr'], client=kwargs['client'])
        else:
            filled_values = {}
            for key, value in connection_data._values.items():
                if value:
                    filled_values[key] = value
            abapclient._connection = Connection(**filled_values)

        try:
            abapclient._connection.call(
                'RFC_FUNCTION_SEARCH', FUNCNAME='DHE2E_CIT_RFC_DATA_ACCESS')
            abapclient._cit_prefix = 'DHE2E'
        except BaseException:
            abapclient._connection.call(
                'RFC_FUNCTION_SEARCH', FUNCNAME='LTE2E_CIT_RFC_DATA_ACCESS')
            abapclient._cit_prefix = 'LTE2E'

        return abapclient

    def _call_cit_rfc(self, dataset_identifier: str, mode, count: int = 0):
        rfcresult = self.call_rfc(
            f'{self._cit_prefix}_CIT_RFC_DATA_ACCESS', IV_TABLE_ID=dataset_identifier, IV_MODE=mode,
            IV_NUM_RECS=str(count))

        self._handle_returncode(rfcresult)

        return rfcresult

    def _handle_returncode(self, rfcresult):
        error_code = rfcresult['EV_RC']
        if error_code != 0:
            error_message = rfcresult['EV_MESSAGE']
            raise RuntimeError(
                f"The ABAP system returned EV_RC={error_code}: {error_message}")

    def get_rowcount(self, dataset_identifier: str) -> int:
        """Gets the number of records in the table with the given dataset 
        identifier."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'C')
        return int(rfcresult['EV_COUNT'])

    def get_tablename(self, dataset_identifier: str) -> str:
        """Get the name of the table with the given dataset identifier."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'C')
        return rfcresult['EV_TABNAME']

    def get_cdsname(self, dataset_identifier: str) -> str:
        """Get the name of CDS view that's based on the table with the given 
        dataset identifier."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'C')
        return rfcresult['EV_CDSNAME']

    def update_records(self, dataset_identifier: str, count: int) -> list[str]:
        """Updates the given count of records in table with the given dataset 
        identifier. The identifiers of the affected records are returned as
        list."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'U', count)
        return rfcresult['ET_RECID']

    def delete_records(self, dataset_identifier: str, count: int) -> list[str]:
        """Deletes the given count of records in table with the given dataset 
        identifier. The identifiers of the deletd records are returned as
        list."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'D', count)
        return rfcresult['ET_RECID']

    def insert_records(self, dataset_identifier: str, count: int) -> list[str]:
        """Inserts the given count of records in table with the given dataset 
        identifier. The identifiers of the inserted records are returned as
        list."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'I', count)
        return rfcresult['ET_RECID']

    def setup_table(self, dataset_identifier: str) -> int:
        """Initializes the contents of the table with the given dataset by
        clearing the contents and generating new records. The number of records
        in the table after initialization is returned."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'S')
        return int(rfcresult['EV_COUNT'])

    def teardown_table(self, dataset_identifier: str) -> bool:
        """Clears the contents of the table with the given dataset identifier."""

        rfcresult = self._call_cit_rfc(dataset_identifier, 'T')
        return int(rfcresult['EV_COUNT']) == 0
