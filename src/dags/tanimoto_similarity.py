import os
import re
from io import BytesIO
from typing import List
from uuid import uuid4

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import select

from utils.alchemy_tables import ChemblIdLookup, CompoundStructures
from utils.calculations import (
    calculate_morgan_fingerprints,
    calculate_tanimoto_similarity,
)

COMPOUND_STRUCTURES_CONNECTION_ID = 'compound_structures_connection_id'
S3_DESTINATION = 's3_destination'
S3_BUCKET_CONNECTION_ID = 's3_connection_id'
S3_BUCKET_NAME = 's3_bucket_name'
S3_INPUT_FILES_SOURCE = 's3_input_files_source'
S3_PRECALCULATED_FINGERPRINTS_SOURCE = 's3_precalculated_similarities_source'


def lower_and_replace_space(string: str) -> str:
    return string.lower().replace(' ', '_')


def clean_molecule_name(string: str) -> str:
    numeric_id = re.match(r'^\D+(\d+)$', string).groups()[0]
    return f'CHEMBL{numeric_id}'


def save_as_parquet_to_s3(
    df: pd.DataFrame, s3_hook: S3Hook, bucket_name: str, key: str
) -> None:
    with BytesIO() as buffer:
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_hook.load_file_obj(buffer, key=key, bucket_name=bucket_name, replace=True)


with DAG(
    dag_id='tanimoto_similarity',
    params={
        COMPOUND_STRUCTURES_CONNECTION_ID: Param(
            type='string',
            default='remote_postgres',
            description='Connection ID of RDB with compound structures',
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
        S3_INPUT_FILES_SOURCE: Param(
            type='string',
            default='final_task/input_files',
            description='Folder in S3 with input files',
        ),
        S3_DESTINATION: Param(
            type='string',
            default='final_task/dmitry_patrushev',
            description='Base destination for output files (prefixes)',
        ),
        S3_PRECALCULATED_FINGERPRINTS_SOURCE: Param(
            type='string',
            default='final_task/dmitry_patrushev/fingerprints',
            description='Source (folder) of precalculated fingerprints',
        ),
    },
    schedule=None,
    catchup=False,
) as tanimoto_similarity:
    start_task = EmptyOperator(task_id='start')

    @task(task_id='get_keys')
    def get_keys_of_input_files(params) -> List[str]:
        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])

        keys = s3_hook.list_keys(
            bucket_name=params[S3_BUCKET_NAME],
            prefix=f'{params[S3_INPUT_FILES_SOURCE]}/*.csv',
            apply_wildcard=True,
        )

        return keys

    @task(task_id='extract_molecule_names')
    def extract_molecule_names(*, params, key) -> List[str]:
        print(f'Reading {key}')

        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])
        file_obj = s3_hook.get_key(key, bucket_name=params[S3_BUCKET_NAME])
        file_content = file_obj.get()['Body'].read()

        df = pd.read_csv(
            BytesIO(file_content),
            usecols=range(2),
            encoding='latin1',
        )

        df.columns = map(lower_and_replace_space, df.columns)

        return df['molecule_name'].apply(clean_molecule_name).to_list()

    @task(task_id='drop_duplicates')
    def drop_duplicates(*, input_molecule_names: List[List[str]]) -> list:
        return list({mn for mnl in input_molecule_names for mn in mnl})

    @task(task_id='get_molecules_data')
    def get_molecules_data(*, params, molecule_names: list) -> List[dict]:
        postgres_hook = PostgresHook(params[COMPOUND_STRUCTURES_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        selection = (
            select(
                ChemblIdLookup.chembl_id,
                ChemblIdLookup.entity_id,
                CompoundStructures.canonical_smiles,
            )
            .join(
                CompoundStructures,
                CompoundStructures.molregno == ChemblIdLookup.entity_id,
            )
            .where(ChemblIdLookup.chembl_id.in_(molecule_names))
        )

        from_database_df = pd.read_sql(selection, engine)

        return from_database_df.to_dict('records')

    @task(task_id='get_all_molecules_fingerprints')
    def get_all_molecules_fingerprints(*, params) -> str:
        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])

        precalculated_fingerprints_keys = s3_hook.list_keys(
            bucket_name=params[S3_BUCKET_NAME],
            prefix=f'{params[S3_PRECALCULATED_FINGERPRINTS_SOURCE]}/*.csv',
            apply_wildcard=True,
        )

        fingerprints = []

        for key in precalculated_fingerprints_keys:
            file_obj = s3_hook.get_key(key, bucket_name=params[S3_BUCKET_NAME])
            file_content = file_obj.get()['Body'].read()

            df = pd.read_csv(BytesIO(file_content))
            fingerprints.extend(df.to_dict('records'))

        temp_file = os.path.join(os.getcwd(), f'tmp/{uuid4()}.csv')
        pd.DataFrame(fingerprints).to_csv(temp_file, index=False)

        return temp_file

    @task(task_id='calculate_similarity')
    def calculate_similarity(
        *, params, target: dict, all_fingerprints_path: str
    ) -> None:
        target_fingerprints = calculate_morgan_fingerprints(target['canonical_smiles'])

        df = pd.read_csv(all_fingerprints_path)
        df['similarity'] = df['morgan_fingerprints_base64'].apply(
            calculate_tanimoto_similarity, args=(target_fingerprints,)
        )
        del df['morgan_fingerprints_base64']

        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])
        key = (
            f'{params[S3_DESTINATION]}/similarity_scores/{target["chembl_id"]}.parquet'
        )
        save_as_parquet_to_s3(df, s3_hook, params[S3_BUCKET_NAME], key)

    @task(task_id='delete_temp_file')
    def delete_temp_file(*, path: str) -> None:
        os.remove(path)

    finish_task = EmptyOperator(task_id='finish')

    raw_molecule_names = extract_molecule_names.expand(key=get_keys_of_input_files())
    molecules_to_handle = get_molecules_data(
        molecule_names=drop_duplicates(input_molecule_names=raw_molecule_names)
    )
    all_fingerprints_path = get_all_molecules_fingerprints()

    (
        start_task
        >> calculate_similarity.partial(
            all_fingerprints_path=all_fingerprints_path
        ).expand(target=molecules_to_handle)
        >> delete_temp_file(path=all_fingerprints_path)
        >> finish_task
    )
