"""
Usage:
    download_dump [options] <wikidb> <day> <hdfs-path>

Checks if a completed MediaWiki XML dump is available,
and if so download it and stream it to HDFS.

Arguments:
    <wikidb>                 The wiki to download, wikidb format, like enwiki
    <day>                    The day to check, yyyyMMdd format
    <hdfs-path>              The hdfs path where to store the downloaded files

Options:
    --name-node=<host>       The host of the cluster name-node
                               [default: http://nn-ia.s3s.altiscale.com:50070]
    --user=<user>            Hdfs user to impersonate
                              (defaults to user running the script)
    --download-threads=<n>   Number of parallel downloading threads
                              [default: 2]
    --download-retries=<n>   Number of retries in case of download failure
                              [default: 3]
    --download-buffer=<b>    Number of bytes for the download buffer
                              [default: 4096]
    --download-timeout=<t>   Number of seconds before timeout while downloading
                               [default: 120]
    -f --force               If set, will delete existing content if any
    -d --debug               Print debug logging
"""
import logging
import os.path
import subprocess
import sys

import docopt
import hdfs
import requests
import re

import Queue
import threading

from hdfs_downloader import HDFSDownloader


logger = logging.getLogger(__name__)


BASE_DUMP_URI_PATTERN = "https://dumps.wikimedia.org/{0}/{1}"
DUMP_STATUS_URI_PATTERN = BASE_DUMP_URI_PATTERN + "/status.html"
DUMP_SHA1_URI_PATTERN = BASE_DUMP_URI_PATTERN + "/{0}-{1}-sha1sums.txt"
DUMP_BZ2_FILE_PATTERN = "{0}-{1}-pages-meta-history.*\.xml.*\.bz2"
DOWNLOAD_FILE_PATTERN = BASE_DUMP_URI_PATTERN + "/{2}"


def main(args):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s -- %(message)s"
    )
    logger.setLevel(logging.DEBUG if args["--debug"] else logging.INFO)

    wikidb = args["<wikidb>"]
    day = args["<day>"]
    hdfs_path = args["<hdfs-path>"]

    name_node = args["--name-node"]
    user = args["--user"]
    num_threads = int(args["--download-threads"])
    num_retries = int(args["--download-retries"])
    buffer_size = int(args["--download-buffer"])
    timeout = int(args["--download-timeout"])

    force = args["--force"]

    logger.debug("Checking day format and values")
    if not check_day_format(day):
        raise RuntimeError("Wrong day format, expected yyyMMdd " +
                           "with valid values.")

    run(wikidb, day, name_node, hdfs_path, user, num_threads, num_retries,
        buffer_size, timeout, force)


def check_day_format(day):
    p = re.compile("(\d{4})(\d\d)(\d\d)")
    match = p.search(day)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        if (year > 2010
           and month > 0 and month < 13
           and day > 0 and day < 32
           and not (month == 2 and day > 29)
           and not (month in [4, 6, 9, 11] and day > 30)):
            return True
    return False


def run(wikidb, day, name_node, hdfs_path, user, num_threads, num_retries,
        buffer_size, timeout, force):

    hdfs_client = hdfs.client.InsecureClient(name_node, user=user)
    if not prepare_hdfs(hdfs_client, hdfs_path, force):
        raise RuntimeError("Problem preparing hdfs")

    hdfs_downloader = HDFSDownloader(name_node, user, num_threads, num_retries,
                                     buffer_size, timeout, force)
    hdfs_downloader.set_logging_level(logger.level)

    if not dump_completed(DUMP_STATUS_URI_PATTERN.format(wikidb, day)):
        raise RuntimeError("Dump not ready to be downloaded from MediaWiki")

    filenames = dump_filenames(DUMP_SHA1_URI_PATTERN.format(wikidb, day),
                               DUMP_BZ2_FILE_PATTERN.format(wikidb, day))

    for filename in filenames:
        url = DOWNLOAD_FILE_PATTERN.format(wikidb, day, filename)
        path = os.path.join(hdfs_path, filename)
        hdfs_downloader.enqueue(url, path)

    hdfs_downloader.download()

    logger.debug("Done !")


def prepare_hdfs(hdfs_client, hdfs_path, force):
    """
    Using hdfs_client, check if hdfs_path exists on hdfs.
    If so, if force, delete existing folder and recreate it.
    """
    if hdfs_client.content(hdfs_path, strict=False):
        # Output path already exists
        if force:
            try:
                hdfs_client.delete(hdfs_path, recursive=True)
                hdfs_client.makedirs(hdfs_path)
                return True
            except hdfs.HdfsError as e:
                logger.error(e)
                return False
        else:
            return False
    else:
        try:
            hdfs_client.makedirs(hdfs_path)
            return True
        except hdfs.HdfsError as e:
            logger.error(e)
            return False


def dump_completed(url):
    logger.debug("Checking for dump completion at {0}".format(url))
    req = requests.get(url)
    return ((req.status_code == 200) and ('Dump complete' in req.text))


def dump_filenames(url, bz2_pattern):
    logger.debug("Getting files list to download {0}".format(url))
    req = requests.get(url)
    filenames = []
    if (req.status_code == 200):
        p = re.compile(bz2_pattern)
        for line in req.text.split('\n'):
            match = p.search(line)
            if match:
                filenames.append(match.group(0))
    return filenames


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
