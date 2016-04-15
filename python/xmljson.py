"""
Usage:
    dump2revdocs [options] <input> <output>

Converts a MediaWiki XML dump to sorted, flattened JSON documents.

Arguments:
    <input>                  The hdfs path to the XML dump input directory
    <output>                 The hdfs path to the revdocs output directory

Options:
    --name-node=<host>       The host of the cluster name-node
                               [default: http://nn-ia.s3s.altiscale.com:50070]
    --user=<user>            Hdfs user to impersonate
                              (defaults to user running the script)
    --queue=<queue>          The hadoop queue in which to run the job
                               [default: research]
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
    -f --force               If set, will overwrite old output
    -d --debug               Print debug logging
"""
import logging
import os.path
import subprocess
import sys

import docopt
import hdfs


logger = logging.getLogger(__name__)


ERROR_MSG = """
        This job has been killed by user

*************************************************
*                                               *
*     You SHOULD double check your cluster !    *
*                                               *
*   The application might still be running...   *
*                                               *
*************************************************
"""


class XMLJSONConverter(object):

    def __init__(self,
                 input_path,
                 output_path,
                 name_node,
                 user,
                 queue,
                 jar,
                 class_,
                 reducers,
                 timeout,
                 mapper_mb,
                 mapper_mb_heap,
                 reducer_mb,
                 reducer_mb_heap,
                 force,
                 debug):

        self.input_path = input_path
        self.output_path = output_path
        self.name_node = name_node
        self.user = user
        self.queue = queue
        self.jar = jar
        self.class_ = class_
        self.reducers = str(int(reducers))
        self.timeout = str(int(timeout) * 1000)
        self.mapper_mb = str(int(mapper_mb))
        self.mapper_mb_heap = str(int(mapper_mb_heap))
        self.reducer_mb = str(int(reducer_mb))
        self.reducer_mb_heap = str(int(reducer_mb_heap))
        self.force = force
        self.debug = debug

        self._init_logging()

    def _init_logging(self):
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(name)s -- %(message)s'
        )
        logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

    def run(self):
        self._configure_hdfs_client()
        if self._prepare_hdfs():
            try:
                self._execute_hadoop_job()
            except KeyboardInterrupt:
                logger.error(ERROR_MSG)
                raise RuntimeError("Job interrupted")
            if not self._check_success_file():
                raise RuntimeError("No success file after hadoop job, " +
                                   "something probably went wrong")

    def _configure_hdfs_client(self):
        name_node = self.name_node
        user = self.user
        self.hdfs_client = hdfs.client.InsecureClient(name_node, user=user)

    def _prepare_hdfs(self):
        logger.debug("Preparing HDFS for xmljson")
        if self._check_success_file():
            if not self.force:
                logger.info("Success file already existing in output path," +
                            " not recomputing")
                return False
        try:
            self.hdfs_client.delete(self.output_path, recursive=True)
        except hdfs.HdfsError as e:
            logger.error(e)
            raise RuntimeError("Problem preparing HDFS.")
        return True

    def _check_success_file(self):
        logger.debug("Checking for success file in {0}".format(
            self.output_path))
        try:
            self.hdfs_client.content(
                os.path.join(self.output_path, "_SUCCESS"))
            return True
        except hdfs.HdfsError:
            return False

    def _execute_hadoop_job(self):
        logger.debug("Starting hadoop job.")
        with open(os.devnull, 'w') as devnull:

            if logger.level == logging.DEBUG:
                stderr = sys.stderr
                stdout = sys.stdout
            else:
                stderr = devnull
                stdout = devnull

            subprocess.call(
                ["hadoop", "jar", self.jar, self.class_,
                 "-Dmapreduce.job.queuename={0}".format(self.queue),
                 "--task-timeout", self.timeout,
                 "--mapper-mb", self.mapper_mb,
                 "--mapper-mb-heap", self.mapper_mb_heap,
                 "--reducer-mb", self.reducer_mb,
                 "--reducer-mb-heap", self.reducer_mb_heap,
                 "--reducers", self.reducers,
                 "--input-path", self.input_path,
                 "--output-path", self.output_path],
                stderr=stderr,
                stdout=stdout)


def main(args):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s -- %(message)s"
    )
    logger.setLevel(logging.DEBUG if args["--debug"] else logging.INFO)

    input_path = args["<input>"]
    output_path = args["<output>"]

    name_node = args["--name-node"]
    user = args["--user"]
    queue = args["--queue"]
    jar = args["--xmljson-jar"]
    class_ = args["--xmljson-class"]
    reducers = args["--xmljson-reducers"]
    timeout = args["--xmljson-timeout"]
    mapper_mb = args["--xmljson-map-mb"]
    mapper_mb_heap = args["--xmljson-map-mb-hp"]
    reducer_mb = args["--xmljson-red-mb"]
    reducer_mb_heap = args["--xmljson-red-mb-hp"]
    force = args["--force"]
    debug = args["--debug"]

    converter = XMLJSONConverter(input_path,
                                 output_path,
                                 name_node,
                                 user,
                                 queue,
                                 jar,
                                 class_,
                                 reducers,
                                 timeout,
                                 mapper_mb,
                                 mapper_mb_heap,
                                 reducer_mb,
                                 reducer_mb_heap,
                                 force,
                                 debug)

    converter.run()


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
