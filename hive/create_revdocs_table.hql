-- Create a hive table for fulltext revdocs in json format.
-- If the table already exists, drop it first.
--
-- Parameters:
--     <revdocs-table>    The fully qualified name of
--                        the revdocs table to create
--     <data-path>        The path to the table data folder
--
-- Usage
--     hive -f create_revdocs_table.hql   \
--          -d revdocs_table=wmf_dumps.enwiki_20150923_fulltext  \
--          -d data_path=/wmf/dumps/full-json-sorted/enwiki-20150923

ADD JAR /opt/hive/hcatalog/share/hcatalog/hive-hcatalog-core-0.13.1.jar;

DROP TABLE IF EXISTS ${revdocs_table};

CREATE EXTERNAL TABLE IF NOT EXISTS ${revdocs_table} (
    id BIGINT,
    timestamp STRING,
    page_id BIGINT,
    page_title STRING,
    page_namespace BIGINT,
    page_redirect STRING,
    page_restrictions ARRAY<STRING>,
    user_id BIGINT,
    user_text STRING,
    minor BOOLEAN,
    comment STRING,
    bytes BIGINT,
    text STRING,
    sha1 STRING,
    parent_id BIGINT,
    model STRING,
    format STRING
)
ROW FORMAT SERDE 'org.apache.hcatalog.data.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${data_path}'
;
