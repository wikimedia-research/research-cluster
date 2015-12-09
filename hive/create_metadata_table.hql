-- Create a hive table for metadata using parquet format
-- If the table already exists, drop it first.
--
-- Parameters:
--     <metadata-table>    The fully qualified name of
--                         the metadata table to create
--     <data-path>         The path to the table data folder
--
-- Usage
--     hive -f create_metadata_table.hql   \
--          -d metadata_table=wmf_dumps.enwiki_20150923  \
--          -d data_path=/wmf/dumps/metadata-parquet/enwiki-20150923

DROP TABLE IF EXISTS ${metadata_table};

CREATE EXTERNAL TABLE IF NOT EXISTS ${metadata_table} (
    id BIGINT,
    timestamp STRING,
    page_id BIGINT,
    page_title STRING,
    page_namespace BIGINT,
    page_redirect STRING,
    page_restrictions ARRAY<STRING>,
    user_id BIGINT,
    user_user_text STRING,
    minor BOOLEAN,
    comment STRING,
    bytes BIGINT,
    sha1 STRING,
    parent_id BIGINT,
    model STRING,
    format STRING
)
STORED AS PARQUET
LOCATION '${data_path}'
;
