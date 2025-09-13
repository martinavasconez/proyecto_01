from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from datetime import datetime
from os import path
import psycopg2
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    if df is None or df.empty:
        print("No hay customers para exportar.")
        return

    host = get_secret_value('pg_host')
    port = get_secret_value('pg_port')
    dbname = get_secret_value('pg_db')
    user = get_secret_value('pg_user')
    password = get_secret_value('pg_password')

    schema = 'raw'
    table = 'qb_customers'
    report_table = 'backfill_report_customers'
    pipeline_name = "qb_customers_backfill"

    # Serializar payloads
    df = df.copy()
    if "payload" in df.columns:
        df["payload"] = df["payload"].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x)
        )
    if "request_payload" in df.columns:
        df["request_payload"] = df["request_payload"].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x)
        )

    started_at = datetime.utcnow()
    print(f"Exportando {len(df)} customers a {schema}.{table} en {host}:{port}/{dbname}")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Crear esquema si no existe
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            # Crear tabla destino
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                id TEXT PRIMARY KEY,
                payload JSONB,
                ingested_at_utc TIMESTAMP,
                extract_window_start_utc TIMESTAMP,
                extract_window_end_utc TIMESTAMP,
                page_number INT,
                page_size INT,
                request_payload TEXT
            );
            """
            cur.execute(ddl)

            # Insert con UPSERT
            insert_sql = f"""
            INSERT INTO {schema}.{table} (
                id, payload, ingested_at_utc,
                extract_window_start_utc, extract_window_end_utc,
                page_number, page_size, request_payload
            )
            VALUES (
                %(id)s, %(payload)s, %(ingested_at_utc)s,
                %(extract_window_start_utc)s, %(extract_window_end_utc)s,
                %(page_number)s, %(page_size)s, %(request_payload)s
            )
            ON CONFLICT (id) DO UPDATE SET
                payload = EXCLUDED.payload,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                extract_window_start_utc = EXCLUDED.extract_window_start_utc,
                extract_window_end_utc = EXCLUDED.extract_window_end_utc,
                page_number = EXCLUDED.page_number,
                page_size = EXCLUDED.page_size,
                request_payload = EXCLUDED.request_payload;
            """
            records = df.to_dict(orient='records')
            for row in records:
                cur.execute(insert_sql, row)

            ddl_report = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{report_table} (
                id SERIAL PRIMARY KEY,
                pipeline_name TEXT,
                entity TEXT,
                row_count INT,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                status TEXT,
                error_message TEXT
            );
            """
            cur.execute(ddl_report)

            ended_at = datetime.utcnow()
            cur.execute(
                f"""
                INSERT INTO {schema}.{report_table} 
                (pipeline_name, entity, row_count, window_start, window_end, 
                 started_at, ended_at, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    pipeline_name,
                    "customers",
                    len(df),
                    df["extract_window_start_utc"].min(),
                    df["extract_window_end_utc"].max(),
                    started_at,
                    ended_at,
                    "success",
                    None,
                )
            )

        conn.commit()
        print(f"Exportación completada: {len(df)} customers procesados.")

    except Exception as e:
        conn.rollback()
        ended_at = datetime.utcnow()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{report_table} (
                    id SERIAL PRIMARY KEY,
                    pipeline_name TEXT,
                    entity TEXT,
                    row_count INT,
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    started_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    status TEXT,
                    error_message TEXT
                );
            """)
            cur.execute(
                f"""
                INSERT INTO {schema}.{report_table} 
                (pipeline_name, entity, row_count, window_start, window_end, 
                 started_at, ended_at, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    pipeline_name,
                    "customers",
                    0,
                    None,
                    None,
                    started_at,
                    ended_at,
                    "error",
                    str(e),
                )
            )
        conn.commit()
        print(f"Error al exportar customers: {e}")
        raise e
    finally:
        conn.close()