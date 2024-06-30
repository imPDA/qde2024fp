import io
import math
import os
import tarfile

from airflow import AirflowException, settings
from airflow.decorators import task, task_group
from airflow.models import DAG, Connection, Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import select
from sqlalchemy.dialects import sqlite
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, PendingRollbackError
from sqlalchemy.orm import Session

from utils.alchemy_tables import Base


def get_class_from_tablename(base, tablename):
    for c in base.__subclasses__():
        if c.__tablename__ == tablename:
            return c


def show_status(status: dict):
    if status['previous_shown'] is None:
        print('Download started!')
        status['previous_shown'] = 0.0
    else:
        if status['progress'] >= status['previous_shown'] + 0.05:
            print(f'Downloaded: {status["progress"]:.0%}')
            status['previous_shown'] = status['progress']


REMOTE_PATH_TO_CHEMBLDB_DIR = '/pub/databases/chembl/ChEMBLdb/releases/chembl_%s/'
DB_ARCHIVE_NAME = 'chembl_%s_sqlite.tar.gz'
DEFAULT_TABLES_TO_INGEST = [
    'chembl_id_lookup',
    'molecule_dictionary',
    'compound_properties',
    'compound_structures',
]


with DAG(
    dag_id='ingest_chembl',
    params={
        'db_version': Param(type='integer', default=34, description='ChEMBLdb version'),
        'tables_to_ingest': Param(
            type='array',
            default=DEFAULT_TABLES_TO_INGEST,
            description='Tables to be transferred from ChEMBLdb to specified RDB',
        ),
    },
    schedule=None,
    catchup=False,
) as ingest_chembl:

    @task(task_id='get_checksum_from_source')
    def get_checksum_from_source(params):
        chembl_hook = FTPHook(ftp_conn_id='chembl')
        path_to_checksums_file = (
            REMOTE_PATH_TO_CHEMBLDB_DIR % params['db_version'] + 'checksums.txt'
        )

        buffer = io.BytesIO()
        chembl_hook.retrieve_file(
            remote_full_path=path_to_checksums_file,
            local_full_path_or_buffer=buffer,
        )
        buffer.seek(0)

        for line in buffer.readlines():
            checksum_line = line.decode()
            if DB_ARCHIVE_NAME % params['db_version'] in checksum_line:
                return checksum_line
        else:
            raise AirflowException('Checksum for db archive was not found')

    @task.bash(
        task_id='calculate_checksum',
        cwd=os.getcwd(),
    )
    def calculate_checksum(params):
        return f'sha256sum /tmp/{DB_ARCHIVE_NAME % params["db_version"]}'

    @task.short_circuit(trigger_rule='none_failed')
    def checksums_are_equal(left, right):
        return left[:64] == right[:64]

    @task_group(
        group_id='verify_checksum',
        default_args={
            'trigger_rule': 'none_failed',
        },
    )
    def verify_checksum():
        checksums_are_equal(get_checksum_from_source(), calculate_checksum())

    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def need_to_load_db(params):
        local_directory = '/tmp/'
        filename = f'chembl_{params["db_version"]}_sqlite.tar.gz'
        path_to_local_file = local_directory + filename

        file_exists = os.path.isfile(path_to_local_file)

        return not file_exists

    @task(task_id='download_sqlite_db_archive')
    def download_sqlite_db_archive(params):
        db_version = params['db_version']
        chembl_hook = FTPHook(ftp_conn_id='chembl')

        filename = DB_ARCHIVE_NAME % db_version
        remote_directory = REMOTE_PATH_TO_CHEMBLDB_DIR % db_version
        path_to_remote_file = remote_directory + filename

        local_directory = '/tmp/'
        path_to_local_file = local_directory + filename

        total_file_size = chembl_hook.get_size(path_to_remote_file)
        output_handle = open(path_to_local_file, 'wb')

        status = {'previous_shown': None, 'progress': 0.0}

        def write_to_file_with_progress(data):
            output_handle.write(data)

            downloaded = output_handle.tell()
            status['progress'] = downloaded / total_file_size

            show_status(status)

        try:
            chembl_hook.retrieve_file(
                remote_full_path=remote_directory,
                local_full_path_or_buffer=path_to_local_file,
                callback=write_to_file_with_progress,
            )
        except UnboundLocalError as e:
            # ignore Airflow bug
            if 'output_handle' not in str(e):
                raise

        output_handle.close()

    @task(task_id='unzip_db_archive')
    def unzip_db_archive(params):
        path_to_file = '/tmp/' + DB_ARCHIVE_NAME % params['db_version']
        with tarfile.open(path_to_file, 'r:gz') as tf:
            tf.extractall('/tmp')

    @task_group(
        group_id='check_connections',
        default_args={
            'trigger_rule': 'none_failed',
        },
    )
    def check_connections():
        @task
        def source_connection_alive(params):
            db_version = params['db_version']
            path_to_sqlite_db = (
                f'/tmp/chembl_{db_version}/chembl_{db_version}_sqlite/'
                f'chembl_{db_version}.db'
            )
            source_connection = Connection(
                conn_id=f'chembl_{db_version}_sqlite',
                conn_type='sqlite',
                host=path_to_sqlite_db,
            )

            session = settings.Session()
            try:
                session.add(source_connection)
                session.commit()
            except PendingRollbackError:
                session.rollback()
            except IntegrityError as e:
                if f'chembl_{db_version}_sqlite' in str(e):
                    session.rollback()
                else:
                    raise

            status, report = source_connection.test_connection()
            if not status:
                raise AirflowException(report)

            return True

        @task
        def destination_connection_alive(params):
            destination_connection = Connection(
                conn_id='test_postgres',  # !!!
                conn_type='postgres',
            )
            status, report = destination_connection.test_connection()
            if not status:
                raise AirflowException(report)

            return True

        @task.short_circuit
        def connections_ok(*connections):
            return all(connections)

        connections_ok(source_connection_alive(), destination_connection_alive())

    @task(task_id='initialize_tables')
    def initialize_tables():
        postgres_hook = PostgresHook('test_postgres')
        engine = postgres_hook.get_sqlalchemy_engine()

        Base.metadata.create_all(engine)

    @task(task_id='transfer_to_postgres')
    def transfer_to_postgres(*, params, table_name: str, offset: int = None):
        table = Base.metadata.tables[table_name]

        # table_class = get_class_from_tablename(Base, table_name)

        sqlite_connection = Connection(
            conn_id=f'chembl_{params["db_version"]}_sqlite',
            conn_type='sqlite',
        )

        source = sqlite_connection.get_hook()
        source_engine: Engine = source.get_sqlalchemy_engine()

        selection = (
            select(table)
            .limit(None)
            .offset(offset)
            .order_by(*table.primary_key.columns)
        )
        selection_string = str(
            selection.compile(
                dialect=sqlite.dialect(),
                compile_kwargs={'literal_binds': True},
            )
        )
        print(f'Selection_string: {selection_string}')

        with Session(source_engine) as session:
            objects = session.execute(selection_string).all()

        print(f'Transferring {len(objects)}')

        destination = PostgresHook(postgres_conn_id='remote_postgres')

        print(f'Object example: {dict(objects[0])}')

        destination_engine: Engine = destination.get_sqlalchemy_engine()

        with Session(destination_engine) as session:
            BUNCH_SIZE = 100000
            bunches = math.ceil(len(objects) / BUNCH_SIZE)

            insertion = insert(table).on_conflict_do_nothing()

            for bunch_i in range(bunches):
                bunch_of_objects = objects[
                    bunch_i * BUNCH_SIZE : (bunch_i + 1) * BUNCH_SIZE
                ]

                print(
                    f'Transferring bunch {bunch_i + 1}/{bunches} '
                    f'({len(bunch_of_objects)} rows)'
                )

                with session.begin():
                    session.execute(insertion, list(map(dict, bunch_of_objects)))

        print('Transfer finished')

    @task(task_id='get_tables_to_ingest')
    def get_tables_to_ingest(params):
        return params['tables_to_ingest']

    finish_task = EmptyOperator(task_id='finish')

    (
        need_to_load_db()
        >> download_sqlite_db_archive()
        >> verify_checksum()
        >> unzip_db_archive()
        >> check_connections()
        >> initialize_tables()
        >> transfer_to_postgres.expand(table_name=get_tables_to_ingest())
        >> finish_task
    )
