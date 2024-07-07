import re
from io import BytesIO

import pandas as pd
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import String, case, cast, desc, func, select, text, tuple_, union
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, aliased
from sqlalchemy_utils.view import CreateView, DropView

from utils.alchemy_tables import (
    Base,
    ChemblIdLookup,
    CompoundProperties,
    MoleculeDictionary,
    MoleculeProperties,
    MoleculeSimilarities,
)
from utils.telegram_notifier import TelegramNotifier

RDB_CONNECTION_ID = 'postgres_connection_id'
S3_BUCKET_CONNECTION_ID = 's3_connection_id'
S3_BUCKET_NAME = 's3_bucket_name'
S3_SIMILARITY_SOURCE = 's3_precalculated_similarities_source'


TEN_RANDOM_MOLECULES = [
    'CHEMBL263076',
    'CHEMBL266458',
    'CHEMBL266223',
    'CHEMBL112998',
    'CHEMBL266885',
    'CHEMBL216458',
    'CHEMBL266457',
    'CHEMBL266886',
    'CHEMBL267864',
    'CHEMBL268150',
]


def replace_on_grand_total(grouped_columns):
    id_ = int('1' * len(grouped_columns), 2)
    full_grouping = text(f'GROUPING{grouped_columns} = {id_}')

    def replace(column):
        return case((full_grouping, 'TOTAL'), else_=cast(column, String))

    return replace


default_args = {'owner': 'imPDA', 'on_failure_callback': TelegramNotifier()}


with DAG(
    dag_id='fill_datamart',
    default_args=default_args,
    params={
        RDB_CONNECTION_ID: Param(
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
        S3_SIMILARITY_SOURCE: Param(
            type='string',
            default='final_task/dmitry_patrushev/similarity_scores',
            description='Source (folder) of precalculated similarity scores',
        ),
    },
    schedule=None,
    catchup=False,
) as fill_datamart:
    start_task = EmptyOperator(task_id='start')

    @task(task_id='get_keys_of_similarity_files')
    def get_keys_of_similarity_files(*, params) -> None:
        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])

        keys = s3_hook.list_keys(
            bucket_name=params[S3_BUCKET_NAME],
            prefix=f'{params[S3_SIMILARITY_SOURCE]}/*.parquet',
            apply_wildcard=True,
        )

        return keys

    @task(task_id='calculate_top_similarities')
    def calculate_top_similarities(*, params, s3_key: str):
        s3_hook = S3Hook(aws_conn_id=params[S3_BUCKET_CONNECTION_ID])
        file_obj = s3_hook.get_key(s3_key, bucket_name=params[S3_BUCKET_NAME])
        file_content = file_obj.get()['Body'].read()

        df = pd.read_parquet(BytesIO(file_content))
        df.sort_values(by=['similarity'], inplace=True, ascending=False)

        top_10_df = df[:10]
        has_duplicates = df.iloc[9]['similarity'] == df.iloc[10]['similarity']

        chembl_id = re.match(r'^[\w/]+/(CHEMBL\d+)\.parquet$', s3_key).groups()[0]

        return chembl_id, top_10_df.to_dict('records'), str(has_duplicates)

    @task(task_id='initialize_tables')
    def initialize_tables(*, params):
        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        Base.metadata.create_all(engine)

        with Session(engine) as session, session.begin():
            session.execute(f'TRUNCATE TABLE {MoleculeProperties.__tablename__}')
            session.execute(f'TRUNCATE TABLE {MoleculeSimilarities.__tablename__}')

    @task(task_id='fill_fact_table')
    def fill_fact_table(*, params, data: tuple):
        source_molecule_chembl_id, records, has_duplicates = data
        has_duplicates = has_duplicates == 'True'

        similarity_df = pd.DataFrame(records)
        similarity_df['source_molecule'] = source_molecule_chembl_id
        similarity_df['has_duplicates_of_last_largest_score'] = has_duplicates

        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        selection = select(ChemblIdLookup.chembl_id, ChemblIdLookup.entity_id).where(
            ChemblIdLookup.entity_id.in_(similarity_df['molregno'].to_list()),
            ChemblIdLookup.entity_type == 'COMPOUND',
        )

        chembl_id_vs_molregno = pd.read_sql(selection, engine)
        similarity_df = similarity_df.set_index('molregno').join(
            chembl_id_vs_molregno.set_index('entity_id'), validate='1:1'
        )

        similarity_df.rename(
            columns={
                'chembl_id': 'target_molecule',
                'similarity': 'similarity_score',
            },
            inplace=True,
        )

        similarity_df.to_sql(
            name=MoleculeSimilarities.__tablename__,
            con=engine,
            if_exists='append',
            index=False,
        )

    @task(task_id='fill_dimension_table')
    def fill_dimension_table(*, params):
        all_molecules = union(
            select(MoleculeSimilarities.source_molecule),
            select(MoleculeSimilarities.target_molecule),
        )

        selection = (
            select(
                ChemblIdLookup.chembl_id,
                MoleculeDictionary.molecule_type,
                CompoundProperties.mw_freebase,
                CompoundProperties.alogp,
                CompoundProperties.psa,
                CompoundProperties.cx_logp,
                CompoundProperties.molecular_species,
                CompoundProperties.full_mwt,
                CompoundProperties.aromatic_rings,
                CompoundProperties.heavy_atoms,
            )
            .join(
                MoleculeDictionary,
                MoleculeDictionary.molregno == ChemblIdLookup.entity_id,
            )
            .join(
                CompoundProperties,
                CompoundProperties.molregno == ChemblIdLookup.entity_id,
            )
            .where(
                ChemblIdLookup.chembl_id.in_(all_molecules),
            )
        )

        insertion = insert(MoleculeProperties).from_select(selection.c, selection)

        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        with Session(engine) as session, session.begin():
            session.execute(insertion)

    @task(task_id='create_average_similarity_view')
    def create_average_similarity_view(*, params):
        raise Exception('Something went wrong')

        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        avg_similarity = select(
            MoleculeSimilarities.source_molecule,
            func.avg(MoleculeSimilarities.similarity_score).label('avg_similarity'),
        ).group_by(MoleculeSimilarities.source_molecule)

        average_similarity_view = CreateView(
            'average_similarity', avg_similarity, replace=True
        )

        with Session(engine) as session, session.begin():
            session.execute(DropView('average_similarity', cascade=False))
            session.execute(average_similarity_view)

    @task(task_id='create_average_alogp_deviation_view')
    def create_average_alogp_deviation_view(*, params):
        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        source_properties = aliased(MoleculeProperties)
        target_properties = aliased(MoleculeProperties)

        avg_alogp_dev = (
            select(
                MoleculeSimilarities.source_molecule,
                func.avg(
                    func.abs(source_properties.alogp - target_properties.alogp)
                ).label('avg_alogp_deviation'),
            )
            .join(
                source_properties,
                source_properties.chembl_id == MoleculeSimilarities.source_molecule,
            )
            .join(
                target_properties,
                target_properties.chembl_id == MoleculeSimilarities.target_molecule,
            )
            .group_by(MoleculeSimilarities.source_molecule)
        )

        average_alogp_deviation_view = CreateView(
            'average_alogp_deviation', avg_alogp_dev, replace=True
        )

        with Session(engine) as session, session.begin():
            session.execute(DropView('average_alogp_deviation', cascade=False))
            session.execute(average_alogp_deviation_view)

    @task(task_id='create_pivoted_table')
    def create_pivoted_table(*, params):
        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        some_molecules = select(
            MoleculeSimilarities.source_molecule,
            MoleculeSimilarities.target_molecule,
            MoleculeSimilarities.similarity_score,
        ).where(MoleculeSimilarities.source_molecule.in_(TEN_RANDOM_MOLECULES))

        df = pd.read_sql(some_molecules, engine)

        pivoted_df = df.pivot(
            index='source_molecule',
            columns='target_molecule',
            values='similarity_score',
        )

        pivoted_df.to_sql(name='pivoted_table', con=engine, if_exists='replace')

    @task(task_id='create_ranked_view')
    def create_ranked_view(*, params):
        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        selection = select(
            MoleculeSimilarities.source_molecule,
            MoleculeSimilarities.target_molecule,
            MoleculeSimilarities.similarity_score,
            func.lead(MoleculeSimilarities.target_molecule, 1)
            .over(
                partition_by=MoleculeSimilarities.source_molecule,
                order_by=desc(MoleculeSimilarities.similarity_score),
            )
            .label('1st_next_chembl_id'),
            func.lead(MoleculeSimilarities.target_molecule, 2)
            .over(
                partition_by=MoleculeSimilarities.source_molecule,
                order_by=desc(MoleculeSimilarities.similarity_score),
            )
            .label('2nd_next_chembl_id'),
        )

        ranked_view = CreateView('ranked_view', selection, replace=True)

        with Session(engine) as session, session.begin():
            session.execute(DropView(ranked_view.name, cascade=False))
            session.execute(ranked_view)

    @task(task_id='create_grouped_averages_view')
    def create_grouped_averages_view(*, params):
        postgres_hook = PostgresHook(params[RDB_CONNECTION_ID])
        engine = postgres_hook.get_sqlalchemy_engine()

        grouped_columns = tuple_(
            MoleculeSimilarities.source_molecule,
            MoleculeProperties.aromatic_rings,
            MoleculeProperties.heavy_atoms,
        )
        replace_ = replace_on_grand_total(grouped_columns)

        selection = (
            select(
                replace_(MoleculeSimilarities.source_molecule).label('source_molecule'),
                replace_(MoleculeProperties.aromatic_rings).label('aromatic_rings'),
                replace_(MoleculeProperties.heavy_atoms).label('heavy_atoms'),
                func.avg(MoleculeSimilarities.similarity_score).label('avg'),
            )
            .join(
                MoleculeProperties,
                MoleculeProperties.chembl_id == MoleculeSimilarities.source_molecule,
            )
            .group_by(
                func.grouping_sets(
                    tuple_(MoleculeSimilarities.source_molecule),
                    tuple_(
                        MoleculeProperties.aromatic_rings,
                        MoleculeProperties.heavy_atoms,
                    ),
                    tuple_(MoleculeProperties.heavy_atoms),
                    tuple_(),
                )
            )
        )

        grouped_averages_view = CreateView(
            'grouped_averages_view', selection, replace=True
        )

        with Session(engine) as session, session.begin():
            session.execute(DropView(grouped_averages_view.name, cascade=False))
            session.execute(grouped_averages_view)

    @task_group(group_id='create_views')
    def create_views():
        create_average_similarity_view()
        create_average_alogp_deviation_view()
        create_pivoted_table()
        create_ranked_view()
        create_grouped_averages_view()

    finish_task = EmptyOperator(task_id='finish_task')
    top_similarities = calculate_top_similarities.expand(
        s3_key=get_keys_of_similarity_files()
    )

    (
        start_task
        >> initialize_tables()
        >> fill_fact_table.expand(data=top_similarities)
        >> fill_dimension_table()
        >> create_views()
        >> finish_task
    )
