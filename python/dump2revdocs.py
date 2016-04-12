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
                               [default: 3600]
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


def main(args):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s -- %(message)s"
    )
    logger.setLevel(logging.DEBUG if args["--debug"] else logging.INFO)

    input_path = args["<input>"]
    output_path = args["<output>"]

    name_node = args["--name-node"]
    queue = args["--queue"]

    jar = args["--xmljson-jar"]
    class_ = args["--xmljson-class"]
    reducers = str(int(args["--xmljson-reducers"]))
    timeout = str(int(args["--xmljson-timeout"]) * 1000)
    mapper_mb = str(int(args["--xmljson-map-mb"]))
    mapper_mb_heap = str(int(args["--xmljson-map-mb-hp"]))
    reducer_mb = str(int(args["--xmljson-red-mb"]))
    reducer_mb_heap = str(int(args["--xmljson-red-mb-hp"]))

    force = args["--force"]

    run(input_path, output_path, queue, name_node, jar, class_, timeout,
        mapper_mb, mapper_mb_heap, reducer_mb, reducer_mb_heap, reducers,
        force)


def run(input_path, output_path, queue, name_node, jar, class_, timeout,
        mapper_mb, mapper_mb_heap, reducer_mb, reducer_mb_heap,
        reducers, force):

    hdfs_client = hdfs.client.InsecureClient(name_node)

    if not force:
        logger.debug("Checking for existing data at {0}.".format(output_path))
        if hdfs_check_success_file(hdfs_client, output_path):
            raise RuntimeError("Already completed.")

    dump2revdocs(input_path, output_path, queue, jar, class_, timeout,
                 mapper_mb, mapper_mb_heap, reducer_mb, reducer_mb_heap,
                 reducers)

    logger.debug("Checking for computed data at {0}.".format(output_path))
    if not hdfs_check_success_file(hdfs_client, output_path):
        raise RuntimeError("Something went wrong.")

    logger.debug("Done !")


def hdfs_check_success_file(hdfs_client, hdfs_path):
    """
    Using hdfs_client, check if hdfs_path contains "_SUCCESS" file.
    """
    try:
        hdfs_client.content(os.path.join(hdfs_path, "_SUCCESS"))
        return True
    except hdfs.HdfsError:
        return False


def dump2revdocs(input_path, output_path, queue, jar, class_, timeout,
                 mapper_mb, mapper_mb_heap, reducer_mb, reducer_mb_heap,
                 reducers):
    logger.debug("Starting hadoop job.")
    with open(os.devnull, 'w') as devnull:
        subprocess.call(["hadoop", "jar", jar, class_,
                         "-Dmapreduce.job.queuename={0}".format(queue),
                         "--task-timeout", timeout,
                         "--mapper-mb", mapper_mb,
                         "--mapper-mb-heap", mapper_mb_heap,
                         "--reducer-mb", reducer_mb,
                         "--reducer-mb-heap", reducer_mb_heap,
                         "--reducers", reducers,
                         "--input-path", input_path,
                         "--output-path", output_path],
                        stderr=(sys.stderr
                                if logger.level == logging.DEBUG
                                else devnull),
                        stdout=(sys.stdout
                                if logger.level == logging.DEBUG
                                else devnull))


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        var
        logger.error("This job has been killed by user hitting CTRL + C.\n\n" +
                     "*" * 48 + "\n*" + " " * 46 + "*\n" +
                     "*    You SHOULD double check your cluster !    *\n" +
                     "*" + " " * 46 + "*\n" +
                     "*   The application might still be running...  *\n" +
                     "*" + " " * 46 + "*\n" + "*" * 48 + "\n\n")
        sys.exit(1)
