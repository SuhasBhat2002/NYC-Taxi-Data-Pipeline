from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(dfs, **kwargs) -> None:
    """
    Export NYC taxi data warehouse datasets to Snowflake.
    Each dataset will be loaded into its corresponding dimension or fact table.

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

    # Connection settings
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    database = 'NYC_TAXI'  # Update with your actual database name
    schema = 'TRIPS'       # Update with your actual schema name

    # Mapping of dataset index -> target table name
    table_mapping = {
        0: 'DATETIME_DIM',
        1: 'PASSENGER_COUNT_DIM',
        2: 'TRIP_DISTANCE_DIM',
        3: 'RATE_CODE_DIM',
        4: 'PICKUP_LOCATION_DIM',
        5: 'DROP_LOCATION_DIM',
        6: 'PAYMENT_TYPE_DIM',
        7: 'FACT_TABLE'
    }

    # Ensure dfs is a list
    if isinstance(dfs, DataFrame):
        dfs = [dfs]

    print(f"📊 Starting export of {len(dfs)} datasets to Snowflake...")
    print(f"🎯 Target: {database}.{schema}")

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Write dimension tables first
        dimension_tables = [0, 1, 2, 3, 4, 5, 6]
        for idx in dimension_tables:
            df = dfs[idx]
            table_name = table_mapping[idx]
            print(f"⏳ Writing dimension table '{table_name}' ({len(df)} rows)...")
            if not df.empty:
                # CORRECTED METHOD CALL
                loader.export(
                    df,
                    table_name,
                    database,
                    schema,
                    if_exists='replace'  # Use 'replace' instead of mode='replace'
                )
            else:
                print(f"⚠️ Dataset {idx} for table '{table_name}' is empty. Skipping.")

        # Write fact table last
        fact_table_index = 7
        df_fact = dfs[fact_table_index]
        table_name = table_mapping[fact_table_index]
        print(f"⏳ Writing fact table '{table_name}' ({len(df_fact)} rows)...")
        if not df_fact.empty:
            # CORRECTED METHOD CALL
            loader.export(
                df_fact,
                table_name,
                database,
                schema,
                if_exists='replace'  # Use 'replace' instead of mode='replace'
            )
        else:
            print(f"⚠️ Fact table dataset is empty. Skipping.")

    print("✅ Export complete!")
