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
    --no-download            If set, doesn't try to download dump, go straight
                              to xmljson extraction
    --no-xmljson             If  set, doesn't try to extract json from xml, go
                              straight to hive tables creation
    --no-fulltext-table      If set, doesn't create hive fulltext table
    --no-metadata-table      If set, doesn't create hive metadata table
    --no-metadata-load       If set, doesn't laod hive metadata table
                               from fulltext one

    --base-path=<path>       Base path where to store the files
                               [default: /wikimedia_data]

    --name-node=<host>       Host of the cluster name-node to use
                               [default: http://nn-ia.s3s.altiscale.com:50070]
    --user=<user>            Hdfs / hive user to impersonate
                               (defaults to user running the script)
    --queue=<queue>          Hadoop queue in which to run the job
                               [default: research]

    --download-type=<d>      Dump type to download, can be 'history' for
                              historical edits or 'current' for current version
                              [default: history]
    --download-no-check      It set, doesn't check md5 of existing or newly
                               downloaded files, assume correctness
    --download-flag=<f>      Name of an empty file created in <hdfs-path> when
                              download is succesfull and checked
                              [default: _SUCCESS]
    --download-checkers=<n>  Number of parallel checking processes
                              [default: 4]
    --download-threads=<n>   Number of parallel downloading threads
                              [default: 2]
    --download-tries=<n>     Number of tries in case of download failure
                              (0 for eternal retries)
                              [default: 0]
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
                               [default: 36000]
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
    --hive-metadata-red=<n>  Number of reducers used to extract metadata from
                               json data (defines the number of output files)
                               [default: 64]
    --hive-metadata-cpr=<c>  Compression scheme to be used for metadata parquet
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

from download_dump import DumpDownloader
from xmljson import XMLJSONConverter
from hql_runner import HQLRunner


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


class WikiDumpEtl(object):

    def __init__(self,
                 no_download,
                 no_xmljson,
                 no_fulltext_table,
                 no_metadata_table,
                 no_metadata_load,
                 wikidb,
                 day,
                 base_path,
                 name_node,
                 user,
                 queue,
                 download_dump_type,
                 download_success_flag,
                 download_no_check,
                 download_num_checkers,
                 download_num_downloaders,
                 download_num_tries,
                 download_buffer_size,
                 download_timeout,
                 xmljson_jar,
                 xmljson_class_,
                 xmljson_reducers,
                 xmljson_timeout,
                 xmljson_mapper_mb,
                 xmljson_mapper_mb_heap,
                 xmljson_reducer_mb,
                 xmljson_reducer_mb_heap,
                 hive_server,
                 hive_port,
                 hive_database,
                 hive_hcatalog,
                 hive_metadata_reducers,
                 hive_metadata_compression,
                 force,
                 debug):

        self.no_download = no_download
        self.no_xmljson = no_xmljson
        self.no_fulltext_table = no_fulltext_table
        self.no_metadata_table = no_metadata_table
        self.no_metadata_load = no_metadata_load

        self.wikidb = wikidb
        self.day = day
        self.base_path = base_path
        self.name_node = name_node
        self.user = user
        self.queue = queue
        self.download_dump_type = download_dump_type
        self.download_success_flag = download_success_flag
        self.download_no_check = download_no_check
        self.download_num_checkers = download_num_checkers
        self.download_num_downloaders = download_num_downloaders
        self.download_num_tries = download_num_tries
        self.download_buffer_size = download_buffer_size
        self.download_timeout = download_timeout
        self.xmljson_jar = xmljson_jar
        self.xmljson_class_ = xmljson_class_
        self.xmljson_reducers = xmljson_reducers
        self.xmljson_timeout = xmljson_timeout
        self.xmljson_mapper_mb = xmljson_mapper_mb
        self.xmljson_mapper_mb_heap = xmljson_mapper_mb_heap
        self.xmljson_reducer_mb = xmljson_reducer_mb
        self.xmljson_reducer_mb_heap = xmljson_reducer_mb_heap
        self.hive_server = hive_server
        self.hive_port = hive_port
        self.hive_database = hive_database
        self.hive_hcatalog = hive_hcatalog
        self.hive_metadata_reducers = hive_metadata_reducers
        self.hive_metadata_compression = hive_metadata_compression
        self.force = force
        self.debug = debug

        self.xml_path = self._wiki_path(FOLDER_XMLBZ2)
        self.json_path = self._wiki_path(FOLDER_JSONBZ2)
        self.meta_path = self._wiki_path(FOLDER_METADATA)

        self.metadata_table = "{0}_{1}".format(self.wikidb, self.day)
        self.fulltext_table = "{0}_fulltext".format(self.metadata_table)

        self._init_logging()

    def _wiki_path(self, postfix):
        return os.path.join(self.base_path,
                            "{0}-{1}".format(self.wikidb, self.day),
                            postfix)

    def _hive_path(self, script):
        # Ugly hack to access hive scripts
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), script)

    def _hive_param(self, param, value):
        return "=".join([param.strip(), value.strip()])

    def _init_logging(self):
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(name)s -- %(message)s'
        )
        logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

    def _download(self):
        logger.debug("Launching dump download")
        downloader = DumpDownloader(self.wikidb,
                                    self.day,
                                    self.xml_path,
                                    self.name_node,
                                    self.user,
                                    self.download_dump_type,
                                    self.download_success_flag,
                                    self.download_no_check,
                                    self.download_num_checkers,
                                    self.download_num_downloaders,
                                    self.download_num_tries,
                                    self.download_buffer_size,
                                    self.download_timeout,
                                    self.force,
                                    self.debug)
        downloader.run()

    def _xmljson(self):
        logger.debug("Launching xmljson conversion")
        converter = XMLJSONConverter(self.xml_path,
                                     self.json_path,
                                     self.name_node,
                                     self.user,
                                     self.queue,
                                     self.xmljson_jar,
                                     self.xmljson_class_,
                                     self.xmljson_reducers,
                                     self.xmljson_timeout,
                                     self.xmljson_mapper_mb,
                                     self.xmljson_mapper_mb_heap,
                                     self.xmljson_reducer_mb,
                                     self.xmljson_reducer_mb_heap,
                                     self.force,
                                     self.debug)
        converter.run()

    def _hive_create_fulltext_table(self):
        logger.debug("Launchung HQLrunner for fulltext table creation")
        hql_path = self._hive_path(HQL_SCRIPT_FULLTEXT_TABLE)
        hql_params = [
            self._hive_param(HQL_PARAM_HCATALOG_PATH, self.hive_hcatalog),
            self._hive_param(HQL_PARAM_FULLTEXT_TABLE, self.fulltext_table),
            self._hive_param(HQL_PARAM_DATA_PATH, self.json_path)
        ]
        hql_runner = HQLRunner(hql_path,
                               hql_params,
                               self.hive_server,
                               self.hive_port,
                               self.user,
                               self.hive_database,
                               self.debug)
        hql_runner.run()

    def _hive_create_metadata_table(self):
        logger.debug("Launching HQLrunner for metadata table creation")
        hql_path = self._hive_path(HQL_SCRIPT_METADATA_TABLE)
        hql_params = [
            self._hive_param(HQL_PARAM_METADATA_TABLE, self.metadata_table),
            self._hive_param(HQL_PARAM_DATA_PATH, self.meta_path)
        ]
        hql_runner = HQLRunner(hql_path,
                               hql_params,
                               self.hive_server,
                               self.hive_port,
                               self.user,
                               self.hive_database,
                               self.debug)
        hql_runner.run()

    def _hive_load_metadata_table(self):
        logger.debug("Launching HQLrunner loading metadata table")
        hql_path = self._hive_path(HQL_SCRIPT_METADATA_LOAD)
        hql_params = [
            self._hive_param(HQL_PARAM_HCATALOG_PATH, self.hive_hcatalog),
            self._hive_param(HQL_PARAM_QUEUE, self.queue),
            self._hive_param(HQL_PARAM_REDUCERS, self.hive_metadata_reducers),
            self._hive_param(HQL_PARAM_COMPRESSION,
                             self.hive_metadata_compression),
            self._hive_param(HQL_PARAM_METADATA_TABLE, self.metadata_table),
            self._hive_param(HQL_PARAM_FULLTEXT_TABLE, self.fulltext_table)
        ]
        hql_runner = HQLRunner(hql_path,
                               hql_params,
                               self.hive_server,
                               self.hive_port,
                               self.user,
                               self.hive_database,
                               self.debug)
        hql_runner.run()

    def run(self):
        if not self.no_download:
            self._download()

        if not self.no_xmljson:
            self._xmljson()

        if not self.no_fulltext_table:
            self._hive_create_fulltext_table()

        if not self.no_metadata_table:
            self._hive_create_metadata_table()

        if not self.no_metadata_load:
            self._hive_load_metadata_table()


def main(args):
    wikidb = args["<wikidb>"]
    day = args["<day>"]

    no_download = args["--no-download"]
    no_xmljson = args["--no-xmljson"]
    no_fulltext_table = args["--no-fulltext-table"]
    no_metadata_table = args["--no-metadata-table"]
    no_metadata_load = args["--no-metadata-load"]
    base_path = args["--base-path"]
    name_node = args["--name-node"]
    user = args["--user"]
    queue = args["--queue"]
    download_dump_type = args["--download-type"]
    download_success_flag = args["--download-flag"]
    download_no_check = args["--download-no-check"]
    download_num_checkers = args["--download-checkers"]
    download_num_downloaders = args["--download-threads"]
    download_num_tries = args["--download-tries"]
    download_buffer_size = args["--download-buffer"]
    download_timeout = args["--download-timeout"]
    xmljson_jar = args["--xmljson-jar"]
    xmljson_class_ = args["--xmljson-class"]
    xmljson_reducers = args["--xmljson-reducers"]
    xmljson_timeout = args["--xmljson-timeout"]
    xmljson_mapper_mb = args["--xmljson-map-mb"]
    xmljson_mapper_mb_heap = args["--xmljson-map-mb-hp"]
    xmljson_reducer_mb = args["--xmljson-red-mb"]
    xmljson_reducer_mb_heap = args["--xmljson-red-mb-hp"]
    hive_server = args["--hive-server"]
    hive_port = args["--hive-port"]
    hive_database = args["--hive-database"]
    hive_hcatalog = args["--hive-hcatalog"]
    hive_metadata_reducers = args["--hive-metadata-red"]
    hive_metadata_compression = args["--hive-metadata-cpr"]
    force = args["--force"]
    debug = args["--debug"]

    etl = WikiDumpEtl(no_download,
                      no_xmljson,
                      no_fulltext_table,
                      no_metadata_table,
                      no_metadata_load,
                      wikidb,
                      day,
                      base_path,
                      name_node,
                      user,
                      queue,
                      download_dump_type,
                      download_success_flag,
                      download_no_check,
                      download_num_checkers,
                      download_num_downloaders,
                      download_num_tries,
                      download_buffer_size,
                      download_timeout,
                      xmljson_jar,
                      xmljson_class_,
                      xmljson_reducers,
                      xmljson_timeout,
                      xmljson_mapper_mb,
                      xmljson_mapper_mb_heap,
                      xmljson_reducer_mb,
                      xmljson_reducer_mb_heap,
                      hive_server,
                      hive_port,
                      hive_database,
                      hive_hcatalog,
                      hive_metadata_reducers,
                      hive_metadata_compression,
                      force,
                      debug)
    etl.run()


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
