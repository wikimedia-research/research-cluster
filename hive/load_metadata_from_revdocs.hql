-- Load a metadata table from a revdocs one
--
-- Parameters:
--     <revdocs_table>      The fully qualified name of the revdocs table
--                          to extract data from
--     <metadata_table>     The fully qualified name of the metadata table
--                          to insert data in
--
-- Usage
--     hive -f load_metadata_from_revdocs.hql          \
--          -d revdocs_table=enwiki_20150923_fulltext  \
--          -d metadata_table=enwiki_20150923

ADD JAR /opt/hive/hcatalog/share/hcatalog/hive-hcatalog-core-0.13.1.jar;

SET parquet.compression      = SNAPPY;
SET mapreduce.job.reduces    = 64;

INSERT OVERWRITE TABLE ${metadata_table}
SELECT
    id,
    timestamp,
    page_id,
    page_title,
    page_namespace,
    page_redirect,
    page_restrictions,
    user_id,
    user_text,
    minor,
    comment,
    bytes,
    sha1,
    parent_id,
    model,
    format
FROM
     ${revdocs_table}
-- Forcing group by to ensure reduce process
GROUP BY
    id,
    timestamp,
    page_id,
    page_title,
    page_namespace,
    page_redirect,
    page_restrictions,
    user_id,
    user_text,
    minor,
    comment,
    bytes,
    sha1,
    parent_id,
    model,
    format
;
