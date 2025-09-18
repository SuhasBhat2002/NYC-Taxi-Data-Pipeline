import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(dfs, **kwargs) -> None:
    """
    Export NYC taxi data warehouse datasets to Snowflake using the direct Snowflake connector.
    This bypasses Mage AI's Snowflake wrapper to avoid version-specific import issues.

    Expected order from transformation pipeline:
    0: datetime_dim
    1: passenger_count_dim
    2: trip_distance_dim
    3: rate_code_dim
    4: pickup_location_dim
    5: drop_location_dim
    6: payment_type_dim
    7: fact_table
    """

    # Config and connection setup
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    database = 'NYC_TAXI'  # Update with your actual database name
    schema = 'PUBLIC'      # Update with your actual schema name

    # Mapping dataset index -> target table name
    table_mapping = {
        0: 'datetime_dim',
        1: 'passenger_count_dim',
        2: 'trip_distance_dim',
        3: 'rate_code_dim',
        4: 'pickup_location_dim',
        5: 'drop_location_dim',
        6: 'payment_type_dim',
        7: 'fact_table',
    }

    # Ensure dfs is a list
    if isinstance(dfs, DataFrame):
        dfs = [dfs]

    print(f"📊 Starting export of {len(dfs)} datasets to Snowflake...")
    print(f"🎯 Target: {database}.{schema}")

    # Load Snowflake configuration
    config_loader = ConfigFileLoader(config_path, config_profile)
    config = config_loader.config

    try:
        # Direct connection to Snowflake
        conn = snowflake.connector.connect(
            account=config['SNOWFLAKE_ACCOUNT'],
            user=config['SNOWFLAKE_USER'],
            password=config['SNOWFLAKE_PASSWORD'],
            warehouse=config.get('SNOWFLAKE_DEFAULT_WH'),
            database=database,
            schema=schema,
            role=config.get('SNOWFLAKE_ROLE'),
            insecure_mode=True,      # skip strict cert validation
            ocsp_fail_open=True,     # allow bypass if OCSP fails
        )
        cursor = conn.cursor()

        # Make sure DB and schema are set
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")

        # Export dimension tables first
        dimension_tables = [0, 1, 2, 3, 4, 5, 6]
        fact_table_index = 7

        for idx in dimension_tables:
            if idx < len(dfs):
                df = dfs[idx]
                table_name = table_mapping[idx]

                print(f"🔄 Exporting dimension table: {table_name} ({len(df)} rows)")

                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    df,
                    table_name,
                    auto_create_table=True,
                    overwrite=True,
                )
                print(f"✅ Exported {schema}.{table_name} ({nrows} rows)")

        # Export fact table last
        if fact_table_index < len(dfs):
            df = dfs[fact_table_index]
            table_name = table_mapping[fact_table_index]

            print(f"🔄 Exporting fact table: {table_name} ({len(df)} rows)")

            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name,
                auto_create_table=True,
                overwrite=True,
            )
            print(f"✅ Exported {schema}.{table_name} ({nrows} rows)")

        # Final summary
        print("🎉 All datasets successfully exported to Snowflake!")
        print("\n📋 Export Summary:")
        print("=" * 50)
        for idx, table_name in table_mapping.items():
            if idx < len(dfs):
                print(f"  {table_name:<25} | {len(dfs[idx]):>10,} rows")
        print("=" * 50)

        # Close connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error during export: {e}")
        print("💡 Suggestion: Verify your Snowflake credentials and network connectivity.")
        import traceback
        traceback.print_exc()
        raise
