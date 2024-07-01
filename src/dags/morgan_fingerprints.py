import io
import math
import re
from io import BytesIO

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from rdkit import Chem
from rdkit.Chem import AllChem
from sqlalchemy import select
from sqlalchemy.orm import Session

from utils.alchemy_tables import CompoundStructures

SOURCE_CONNECTION_ID = 'source_connection_id'
S3_DESTINATION = 's3_destination'
S3_BUCKET_CONNECTION_ID = 's3_connection_id'
S3_BUCKET_NAME = 's3_bucket_name'
MAX_ROWS_PER_BUNCH = 'max_rows_per_bunch'


def calculate_morgan_fingerprints_base64(smiles: str) -> str:
    fingerprint_generator = AllChem.GetMorganGenerator(radius=2, fpSize=2048)
    try:
        molecule = Chem.MolFromSmiles(smiles)
        fingerprint = fingerprint_generator.GetFingerprint(molecule)
    except Exception as e:
        print(f'Error occurred: {e}')

        return ''

    return fingerprint.ToBase64()


def calculate_morgan_fingerprints_from_inchi_base64(inchi: str) -> str:
    fingerprint_generator = AllChem.GetMorganGenerator(radius=2, fpSize=2048)
    try:
        molecule = Chem.MolFromInchi(inchi)
        fingerprint = fingerprint_generator.GetFingerprint(molecule)
    except Exception as e:
        print(f'Error occurred: {e}')

        return ''

    return fingerprint.ToBase64()


def save_df_to_s3(
    df: pd.DataFrame, s3_hook: S3Hook, bucket_name: str, key: str
) -> None:
    with io.BytesIO() as buffer:
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        s3_hook.load_file_obj(buffer, key=key, bucket_name=bucket_name, replace=True)


with DAG(
    dag_id='morgan_fingerprints',
    params={
        SOURCE_CONNECTION_ID: Param(
            type='string',
            default='remote_postgres',
            description='Connection ID of RDB from which data will be read from',
        ),
        S3_BUCKET_CONNECTION_ID: Param(
            type='string',
            default='de-school-2024-aws-s3',
            description='Connection ID of S3 bucket to work with',
        ),
        S3_BUCKET_NAME: Param(
            type='string',
            default='de-school-2024-aws',
            description='Bucket name',
        ),
        S3_DESTINATION: Param(
            type='string',
            default='final_task/dmitry_patrushev',
            description='Base destination for output files (prefixes)',
        ),
        MAX_ROWS_PER_BUNCH: Param(
            type='integer',
            default=100000,
            description='How much rows will be calculated per bunch (per task)',
        ),
    },
    schedule=None,
    catchup=False,
) as morgan_fingerprints:
    start_task = EmptyOperator(task_id='start')

    @task(task_id='split_by_bunches')
    def split_by_bunches(*, params):
        max_rows_per_bunch = params[MAX_ROWS_PER_BUNCH]
        postgres_hook = PostgresHook(params[SOURCE_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        with Session(engine) as session:
            rows = session.query(CompoundStructures).count()

        bunches = math.ceil(rows / max_rows_per_bunch)

        print(f'{bunches} bunches total with {max_rows_per_bunch} rows each')

        return [
            {
                'offset': i * max_rows_per_bunch,
                'limit': max_rows_per_bunch,
            }
            for i in range(bunches)
        ]

    @task(task_id='calculate_morgan_fingerprints_for_rows')
    def calculate_morgan_fingerprints_for_rows(*, params, bunch: dict):
        postgres_hook = PostgresHook(params[SOURCE_CONNECTION_ID])

        offset = bunch['offset']
        limit = bunch['limit']

        pk_columns = CompoundStructures.__table__.primary_key.columns
        selection = (
            select(
                *pk_columns,
                CompoundStructures.canonical_smiles,
            )
            .order_by(*pk_columns)
            .limit(limit)
            .offset(offset)
        )

        engine = postgres_hook.get_sqlalchemy_engine()
        df = pd.read_sql(selection, engine)

        df['morgan_fingerprints_base64'] = df['canonical_smiles'].apply(
            calculate_morgan_fingerprints_base64
        )
        del df['canonical_smiles']

        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])
        fingerprints_destination = f'{params[S3_DESTINATION]}/fingerprints'
        bucket_name = params[S3_BUCKET_NAME]

        empty_fingerprints = df['morgan_fingerprints_base64'] == ''

        good_df = df[~empty_fingerprints]
        if num_good := len(good_df):
            key = f'{fingerprints_destination}/morgan_fingerprints_{offset}.csv'
            save_df_to_s3(good_df, s3_hook, bucket_name, key)
            print(f'Writing {num_good} calculated Morgan fingerprints to {key}')

        bad_df = df[empty_fingerprints]
        if num_bad := len(bad_df):
            key = f'{fingerprints_destination}/morgan_fingerprints_{offset}.bad.csv'
            save_df_to_s3(bad_df, s3_hook, bucket_name, key)
            print(f'{num_bad} unprocessed rows saved to {key}')

    @task(task_id='try_to_recalculate_errors')
    def try_to_recalculate_errors(params):
        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])
        bad_fingerprints = (
            f'{params[S3_DESTINATION]}/fingerprints/morgan_fingerprints_*.bad.csv'
        )

        bad_keys = s3_hook.list_keys(
            bucket_name=params[S3_BUCKET_NAME],
            prefix=bad_fingerprints,
            apply_wildcard=True,
        )

        if not bad_keys:
            return 'No bad keys to recalculate'

        postgres_hook = PostgresHook(params[SOURCE_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        for bad_key in bad_keys:
            file_obj = s3_hook.get_key(
                bad_key,
                bucket_name=params[S3_BUCKET_NAME],
            )
            file_content = file_obj.get()['Body'].read()
            bad_df = pd.read_csv(BytesIO(file_content))

            bad_molregno = bad_df['molregno'].to_list()

            offset = int(
                re.match(r'^.*morgan_fingerprints_(\d+)\.bad\.csv$', bad_key).groups()[
                    0
                ]
            )

            pk_columns = CompoundStructures.__table__.primary_key.columns
            selection = select(
                *pk_columns,
                CompoundStructures.standard_inchi,
            ).where(CompoundStructures.molregno.in_(bad_molregno))
            rows_from_database_df = pd.read_sql(selection, engine)

            rows_from_database_df['morgan_fingerprints_base64'] = rows_from_database_df[
                'standard_inchi'
            ].apply(calculate_morgan_fingerprints_from_inchi_base64)
            del rows_from_database_df['standard_inchi']
            empty_fingerprints = (
                rows_from_database_df['morgan_fingerprints_base64'] == ''
            )

            if len(rows_from_database_df[~empty_fingerprints]) == 0:
                print(f'No errors was fixed in {bad_key}')
                continue

            new_good_df = rows_from_database_df[~empty_fingerprints]

            file_obj = s3_hook.get_key(
                f'{params[S3_DESTINATION]}/fingerprints/morgan_fingerprints_{offset}.csv',
                bucket_name=params[S3_BUCKET_NAME],
            )
            file_content = file_obj.get()['Body'].read()
            good_df = pd.read_csv(BytesIO(file_content))

            good_df = pd.concat([good_df, new_good_df])

            fingerprints_destination = f'{params[S3_DESTINATION]}/fingerprints'
            bucket_name = params[S3_BUCKET_NAME]

            if num_good := len(good_df):
                key = f'{fingerprints_destination}/morgan_fingerprints_{offset}.csv'
                save_df_to_s3(good_df, s3_hook, bucket_name, key)
                print(f'Writing {num_good} calculated Morgan fingerprints to {key}')

            bad_df = rows_from_database_df[empty_fingerprints]
            if num_bad := len(bad_df):
                key = f'{fingerprints_destination}/morgan_fingerprints_{offset}.bad.csv'
                save_df_to_s3(bad_df, s3_hook, bucket_name, key)
                print(f'{num_bad} unprocessed rows saved to {key}')
            else:
                s3_hook.delete_objects(
                    bucket=bucket_name,
                    keys=[
                        bad_key,
                    ],
                )
                print('All errors was fixed!')

    finish_task = EmptyOperator(task_id='finish')

    (
        start_task
        >> calculate_morgan_fingerprints_for_rows.expand(bunch=split_by_bunches())
        >> try_to_recalculate_errors()
        >> finish_task
    )
