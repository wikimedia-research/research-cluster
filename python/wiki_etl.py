"""
Usage:
    wiki_etl [options] <wikidb> <day>

Download an XML dump (if available), converts it to json (sorting revisions
by timestamp for each page), then create and load associated fulltext and
metadata hive tables (default values are set for enwiki size) .

xml dump path:     <base-path>/<wikidb>-<day>/xmlbz2
sorted json path:  <base-path>/<wikidb>-<day>/jsonbz2
metadata path:     <base-path>/<wikidb>-<day>/meta_parquet

hive fulltext table: <db>.<wikidb>_<day>_fulltext
hive metadata table: <db>.<wikidb>_<day>

Arguments:
    <wikidb>                 The wiki to process, wikidb format, like enwiki
    <day>                    The day to check dump for, yyyyMMdd format

Options:
    --base-path=<path>       Base path where to store the files
                               [default: /wikimedia_data]

    --name-node=<host>       Host of the cluster name-node to use
                               [default: http://nn-ia.s3s.altiscale.com:50070]
    --user=<user>            Hdfs / hive user to impersonate
                               (defaults to user running the script)
    --queue=<queue>          Hadoop queue in which to run the job
                               [default: research]

    --download-threads=<n>   Number of parallel downloading threads
                              [default: 2]
    --download-retries=<n>   Number of retries in case of download failure
                              [default: 3]
    --download-buffer=<b>    Number of bytes for the download buffer
                              [default: 4096]
    --download-timeout=<t>   Number of seconds before timeout while downloading
                               [default: 120]

    --xmljson-jar=<path>     XML to JSON mapreduce job jar path
                               [default: /wmf/jars/wikihadoop-0.2.jar]
    --xmljson-class=<name>   XML to JSON mapreduce job class
                               [default: org.wikimedia.wikihadoop.job.JsonRevisionsSortedPerPage]
    --xmljson-reducers=<n>   XML to JSON mapreduce number of reducers
                               (defines the maximum number of output files)
                               [default: 2000]
    --xmljson-timeout=<s>    XML to JSON mapreduce task timeout in seconds
                               [default: 3600]
    --xmljson-map-mb=<n>     Mapper memory in Yarn  (MB)  [default: 2048]
    --xmljson-map-mb-hp=<n>  Mapper memory in JVM   (MB)  [default: 1792]
    --xmljson-red-mb=<n>     Reducer memory in Yarn (MB)  [default: 4096]
    --xmljson-red-mb-hp=<n>  Reducer memory in JVM  (MB)  [default: 3584]

    --hive-server=<host>     Host of the Hive server to use
                               [default: hiveserver-ia.s3s.altiscale.com]
    --hive-port=<port>       Port of the hive server to use
                               [default: 10000]
    --hive-database=<db>     Hive database against which to run the script
                               [default: wmf_dumps]
    --hive-hcatalog=<path>   Path to the hive-hcatalog-core jar file
                               [default: /opt/hive/hcatalog/share/hcatalog/hive-hcatalog-core-0.13.1.jar]
    --metadata-reducers=<n>  Number of reducers used to extract metadata from
                               json data (defines the number of output files)
                               [default: 64]
    --metadata-compress=<c>  Compression scheme to be used for metadata parquet
                               table (can be SNAPPY or GZIP).
                               [default: SNAPPY]
    -f --force               If set, will delete existing content if any
    -d --debug               Print debug logging
"""
import os
import sys
import logging
import copy

import docopt

import download_dump
import dump2revdocs
import hql_runner


logger = logging.getLogger(__name__)


FOLDER_XMLBZ2 = "xmlbz2"
FOLDER_JSONBZ2 = "jsonbz2"
FOLDER_METADATA = "meta_parquet"

HQL_SCRIPT_FULLTEXT_TABLE = "../hive/create_revdocs_table.hql"
HQL_SCRIPT_METADATA_TABLE = "../hive/create_metadata_table.hql"
HQL_SCRIPT_METADATA_LOAD = "../hive/load_metadata_from_revdocs.hql"

HQL_PARAM_HCATALOG_PATH = "hcatalog_path"
HQL_PARAM_FULLTEXT_TABLE = "revdocs_table"
HQL_PARAM_METADATA_TABLE = "metadata_table"
HQL_PARAM_DATA_PATH = "data_path"
HQL_PARAM_QUEUE = "queue"
HQL_PARAM_REDUCERS = "reducers"
HQL_PARAM_COMPRESSION = "compression"


def main(args):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s -- %(message)s"
    )
    logger.setLevel(logging.DEBUG if args["--debug"] else logging.INFO)

    base_path = args["--base-path"]
    wikidb = args["<wikidb>"]
    day = args["<day>"]
    hive_hcatalog = args["--hive-hcatalog"]
    queue = args["--queue"]
    metadata_reducers = args["--metadata-reducers"]
    metadata_compression = args["--metadata-compress"]

    wiki_path = lambda d: os.path.join(base_path, "{0}-{1}".format(
        wikidb, day), d)
    # Ugly hack to access hive scripts
    hive_path = lambda s: os.path.join(
        os.path.dirname(os.path.abspath(__file__)), s)
    param_val = lambda p, v: "=".join([p.strip(), v.strip()])
    metadata_table = "{0}_{1}".format(wikidb, day)
    fulltext_table = "{0}_fulltext".format(metadata_table)

    wiki_xml = wiki_path(FOLDER_XMLBZ2)
    wiki_json = wiki_path(FOLDER_JSONBZ2)
    wiki_meta = wiki_path(FOLDER_METADATA)

    logger.debug("Launching download_dump")
    download_dump_args = copy.copy(args)
    download_dump_args["<hdfs-path>"] = wiki_xml
    download_dump.main(download_dump_args)

    logger.debug("Launching dump2revdocs")
    dump2revdocs_args = copy.copy(args)
    dump2revdocs_args["<input>"] = wiki_xml
    dump2revdocs_args["<output>"] = wiki_json
    dump2revdocs.main(dump2revdocs_args)

    logger.debug("Launch hql_runner.main for fulltext table creation")
    hql_fulltext_table_args = copy.copy(args)
    hql_fulltext_table_args["<hql>"] = hive_path(HQL_SCRIPT_FULLTEXT_TABLE)
    hql_fulltext_table_args["<param>"] = [
        param_val(HQL_PARAM_HCATALOG_PATH, hive_hcatalog),
        param_val(HQL_PARAM_FULLTEXT_TABLE, fulltext_table),
        param_val(HQL_PARAM_DATA_PATH, wiki_json)
    ]
    hql_runner.main(hql_fulltext_table_args)

    logger.debug("Launch hql_runner.main for metadata table creation")
    hql_metadata_table_args = copy.copy(args)
    hql_metadata_table_args["<hql>"] = hive_path(HQL_SCRIPT_METADATA_TABLE)
    hql_metadata_table_args["<param>"] = [
        param_val(HQL_PARAM_METADATA_TABLE, metadata_table),
        param_val(HQL_PARAM_DATA_PATH, wiki_meta)
    ]
    hql_runner.main(hql_metadata_table_args)

    logger.debug("Launch hql_runner.main for metadata table load")
    hql_metadata_load_args = copy.copy(args)
    hql_metadata_load_args["<hql>"] = hive_path(HQL_SCRIPT_METADATA_LOAD)
    hql_metadata_load_args["<param>"] = [
        param_val(HQL_PARAM_HCATALOG_PATH, hive_hcatalog),
        param_val(HQL_PARAM_QUEUE, queue),
        param_val(HQL_PARAM_REDUCERS, metadata_reducers),
        param_val(HQL_PARAM_COMPRESSION, metadata_compression),
        param_val(HQL_PARAM_METADATA_TABLE, metadata_table),
        param_val(HQL_PARAM_FULLTEXT_TABLE, fulltext_table)
    ]
    hql_runner.main(hql_metadata_load_args)


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
