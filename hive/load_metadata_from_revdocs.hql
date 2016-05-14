-- Load a metadata table from a revdocs one
--
-- Parameters:
--     <hcatalog_path>       The path where to find hive-hcatalog-core jar
--                             on your cluster (needed for handling json)
--     <queue>               The hadoop queue onto which the job should be run
--                             If you don't know, 'default' might do
--     <reducers>            The number of reducers of the job, that defines
--                             the number of files to output
--     <compression>         The compression scheme to be used for parquet
--                             files (usual values are SNAPPY and GZIP)
--     <revdocs_table>       The fully qualified name of the revdocs table
--                             to extract data from
--     <metadata_table>      The fully qualified name of the metadata table
--                             to insert data in

--
-- Usage
--     hive -f load_metadata_from_revdocs.hql          \
--          -d hcatalog_path=/opt/hive/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.0.jar \
--          -d metastore_path=/opt/hive/lib/hive-metastore-1.2.0.jar \
--          -d queue=default                           \
--          -d reducers=64                             \
--          -d compression=SNAPPY                      \
--          -d revdocs_table=enwiki_20150923_fulltext  \
--          -d metadata_table=enwiki_20150923          \


ADD JAR ${hcatalog_path};
ADD JAR ${metastore_path};


SET mapred.job.queue.name    = ${queue};
SET mapreduce.job.reduces    = ${reducers};
SET parquet.compression      = ${compression};


INSERT OVERWRITE TABLE ${metadata_table}
SELECT
    id,
    timestamp,
    page_id,
    page_title,
    page_namespace,
    page_redirect,
    CASE WHEN size(page_restrictions) = 0
        THEN NULL
        ELSE page_restrictions
    END AS page_restrictions,
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
    CASE WHEN size(page_restrictions) = 0
        THEN NULL
        ELSE page_restrictions
    END,
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
