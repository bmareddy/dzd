import pandas as pd
from extract import Extract
import duckdb
import re
import uuid

###################################
# PROCESS COLLECTIONS DATA
###################################

# Extract collections data
RUN_ID = uuid.uuid4()
ROOT_DIR = 'dzd'
SOURCE_DIR = 'source_data'
OUTPUT_DIR = 'output_data'
COLLECTIONS_FILE_PATH = f'{ROOT_DIR}\{SOURCE_DIR}\CollectionsData.csv'
collections_source = Extract('collections', COLLECTIONS_FILE_PATH)
df_raw_collections = collections_source.get_source_data()

# Validate collections data
# TODO: Implement validations on collections data
#       1. Not Null tests
#       2. Data type tests
#       3. Data value tests

# Transform and clean collections data
df_clean_collections = duckdb.query(
    """SELECT trim(sampid) sampid, 
                trim(hid) hid, 
                concat('ISO',cast(coalesce(cast(isolate AS INT), 1) AS VARCHAR)) AS isolate,
                trim(date_collected) date_collected
        FROM df_raw_collections
    """
    ).to_df()
df_clean_collections['date_collected'] = pd.to_datetime(df_clean_collections['date_collected'])

print("########## Collections ############")
print(df_clean_collections.info())
print(df_clean_collections.head())

# TODO: In the production version of the pipeline, this is where we would insert into the archive table

df_clean_collections = duckdb.query(
    """
    WITH ordered AS (
        SELECT sampid, hid, isolate, date_collected,
                ROW_NUMBER() OVER (PARTITION BY hid, isolate ORDER BY date_collected DESC) AS rank
    FROM df_clean_collections
    )
    SELECT sampid, hid, isolate, date_collected
    FROM ordered
    WHERE rank = 1
    """
).to_df()

###################################
# PROCESS PHENOTYPE DATA
###################################

# Extract Phenotype data
PHENO_FILE_PATH = 'dzd\source_data\PhenotypeData.csv'
phenotype_source = Extract('phenotype', PHENO_FILE_PATH)
df_raw_phenotype = phenotype_source.get_source_data()

# Validate Phenotype data
# TODO: Implement validations on Phenotype data
#       1. Not Null tests
#       2. Data type tests
#       3. Data value tests - fail on dupes, new values in category fields etc.

# Generate dim_antibiotics
def gen_key_from_string(in_str):
    """
    converts input string to lowercase
    and strips any characters except alphanumeric
    """
    out_str = in_str.lower()
    return re.sub('[^a-z0-9]', '', out_str)

df_raw_phenotype["antibiotic_join_key"] = df_raw_phenotype["antibiotic"].apply(gen_key_from_string)
df_distinct_antibiotics = duckdb.query("SELECT DISTINCT antibiotic AS name, antibiotic_join_key AS source_join_key FROM df_raw_phenotype").to_df()

# -- picks one name per join key in case there are multiple names that are similar
df_distinct_antibiotics = duckdb.query(
    """
    SELECT source_join_key, MAX(name) AS name
    FROM df_distinct_antibiotics
    GROUP BY 1
    """
).to_df()

# Notes:
# In prod, this would be a stage table
# And net new antibiotics will be identified using a left join to `dim_antibiotics`
# If there are net new antibiotics, a human-in-the-loop process can review and approve the drugs to 
# ensure naming consistency is maintained, and even a clean name is assigned
# and are then added back to the dim table thereby always maintaining a unique list of drugs

df_dim_antibiotics = duckdb.query("""
            SELECT
                gen_random_uuid() AS key,
                name,
                CAST(NULL AS VARCHAR) AS clean_name,
                source_join_key,
                get_current_time() AS created_at,
                get_current_time() AS updated_at
            FROM df_distinct_antibiotics
    """).to_df()

df_dim_antibiotics.to_csv(f'{ROOT_DIR}\{OUTPUT_DIR}\dim_antibiotics.csv', header=True, encoding='utf-8')

# Generate fct
df_clean_phenotype = duckdb.query(
    """
    SELECT
        trim(hid) AS hid,
        trim(isolate) AS isolate,
        strptime(trim(received), '%m/%d/%Y %H%M') AS received,
        trim(organism) AS organism,
        coalesce(lower(trim(source)), 'unknown') AS source,
        lower(trim(test)) AS test,
        trim(antibiotic) AS antibiotic,
        antibiotic_join_key,
        trim(value) AS value,
        coalesce(upper(trim(antibiotic_interpretaion)), 'NOT DETERMINED') AS antibiotic_interpretation,
        str_split(trim(method), ';') AS method
    FROM df_raw_phenotype
    """
).to_df()

print("########## Phenotype ############")
print(df_clean_phenotype.info())
print(df_clean_phenotype.head())

df_stg = duckdb.query(
    f"""
    SELECT 
        gen_random_uuid() AS key,
        today() AS etl_date,
        '{RUN_ID}' AS etl_runid,
        c.sampid AS dzdid,
        a.key AS antibiotic_key,
        p.received AS received_ts,
        c.date_collected AS collected_ts,
        p.value,
        p.antibiotic_interpretation,
        p.organism,
        p.method,
        p.test,
        p.source
    FROM df_clean_phenotype p
    JOIN df_clean_collections c
        ON p.hid = c.hid
        AND p.isolate = c.isolate
    JOIN df_dim_antibiotics a
        ON p.antibiotic_join_key = a.source_join_key
    """
).to_df()

# TODO: validate that there is no data loss due to joins

df_stg.to_csv(f"{ROOT_DIR}\{OUTPUT_DIR}\\fct_phenodata_results.csv", header=True, encoding='utf-8', index=False)

print("########## Fact ############")
print(df_stg.info())
print(df_stg.head())

# Generate insights

# Notes: Ideally, the source for this would be fct table, but to simplify I'm using `df_stg` instead 
# since it is equal to fct data set

df_insights = duckdb.query(
    """
    SELECT
        f.dzdid,
        coalesce(a.clean_name, name) AS antibiotic_name,
        f.organism,
        f.antibiotic_interpretation,
        count(1) AS frequency,
        max(collected_ts) AS latest_collection_at
    FROM df_stg f
    JOIN df_dim_antibiotics a
        ON f.antibiotic_key = a.key
    GROUP BY 1,2,3,4
    """
).to_df()

df_insights.to_csv(f"{ROOT_DIR}\{OUTPUT_DIR}\insights_phenotype.csv", header=True,  encoding='utf-8', index=False)

print("########## Insights ############")
print(df_insights.info())
print(df_insights.head())
