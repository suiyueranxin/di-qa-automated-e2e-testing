from pyrfc import Connection


def create_connection():
    connection = Connection(
        # TODO: Provide valid connection details
        user='', passwd='', ashost='', sysnr='', client='')
    return connection


def handle_returncode(rfc_result):
    error_code = rfc_result['EV_RC']
    if error_code != 0:
        error_message = rfc_result['EV_MESSAGE']
        raise RuntimeError(
            f"The ABAP system returned EV_RC={error_code}: {error_message}")


def get_count(dataset_identifier):
    connection = create_connection()

    rfc_result = connection.call(
        'DHE2E_CIT_RFC_DATA_ACCESS', IV_TABLE_ID=dataset_identifier, IV_MODE='C')

    handle_returncode(rfc_result)

    return int(rfc_result['EV_COUNT'])


def get_table_and_cds_name(dataset_identifier):
    connection = create_connection()

    rfc_result = connection.call(
        'DHE2E_CIT_RFC_DATA_ACCESS', IV_TABLE_ID=dataset_identifier, IV_MODE='C')

    handle_returncode(rfc_result)

    return rfc_result['EV_TABNAME'], rfc_result['EV_CDSNAME']


def update_records(dataset_identifier, count):
    connection = create_connection()

    rfc_result = connection.call(
        'DHE2E_CIT_RFC_DATA_ACCESS', IV_TABLE_ID=dataset_identifier, IV_MODE='U', IV_NUM_RECS=str(count))

    handle_returncode(rfc_result)

    return rfc_result['ET_RECID']


def delete_records(dataset_identifier, count):
    connection = create_connection()

    rfc_result = connection.call(
        'DHE2E_CIT_RFC_DATA_ACCESS', IV_TABLE_ID=dataset_identifier, IV_MODE='D', IV_NUM_RECS=str(count))

    handle_returncode(rfc_result)

    return rfc_result['ET_RECID']


def insert_records(dataset_identifier, count):
    connection = create_connection()

    rfc_result = connection.call(
        'DHE2E_CIT_RFC_DATA_ACCESS', IV_TABLE_ID=dataset_identifier, IV_MODE='I', IV_NUM_RECS=str(count))

    handle_returncode(rfc_result)

    return rfc_result['ET_RECID']
