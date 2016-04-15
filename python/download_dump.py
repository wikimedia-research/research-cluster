"""
Usage:
    download_dump [options] <wikidb> <day> <hdfs-path>

Checks if a completed MediaWiki XML dump is available,
and if so download it and stream it to HDFS by completing
existing partial data if any.

Arguments:
    <wikidb>                 The wiki to download, wikidb format, like enwiki
    <day>                    The day to check, yyyyMMdd format
    <hdfs-path>              The hdfs path where to store the downloaded files

Options:
    --name-node=<host>       The host of the cluster name-node
                               [default: http://nn-ia.s3s.altiscale.com:50070]
    --user=<user>            Hdfs user to impersonate
                              (defaults to user running the script)
    --download-no-check      It set, doesn't check md5 of existing or newly
                               downloaded files, assume correctness
    --download-flag=<f>      Name of an empty file created in <hdfs-path> when
                              download is succesfull and checked
                              [default: _SUCCESS]
    --download-threads=<n>   Number of parallel downloading threads
                              [default: 2]
    --download-tries=<n>     Number of tries in case of download failure
                              (0 for eternal retries)
                              [default: 0]
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
import hashlib
import re

import Queue
import threading

from hdfs_downloader import HDFSDownloader


logger = logging.getLogger(__name__)


BASE_DUMP_URI_PATTERN = "https://dumps.wikimedia.org/{0}/{1}"
DUMP_STATUS_URI_PATTERN = BASE_DUMP_URI_PATTERN + "/status.html"
DUMP_MD5_URI_PATTERN = BASE_DUMP_URI_PATTERN + "/{0}-{1}-md5sums.txt"
DUMP_BZ2_FILE_PATTERN = ("(\w{{32}})  " +
                         "({0}-{1}-pages-meta-history.*\.xml.*\.bz2)")
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
    flag = args["--download-flag"]
    no_check = args["--download-no-check"]
    num_threads = int(args["--download-threads"])
    num_tries = int(args["--download-tries"])
    buffer_size = int(args["--download-buffer"])
    timeout = int(args["--download-timeout"])

    force = args["--force"]

    logger.debug("Checking day format and values")
    if not check_day_format(day):
        raise RuntimeError("Wrong day format, expected yyyMMdd " +
                           "with valid values.")

    run(wikidb, day, name_node, hdfs_path, user, flag, no_check, num_threads,
        num_tries, buffer_size, timeout, force)


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


def run(wikidb, day, name_node, hdfs_path, user, flag, no_check, num_threads,
        num_tries, buffer_size, timeout, force):

    hdfs_client = hdfs.client.InsecureClient(name_node, user=user)

    if not dump_completed(DUMP_STATUS_URI_PATTERN.format(wikidb, day)):
        raise RuntimeError("Dump not ready to be downloaded from MediaWiki")

    files_info = dump_files_infos(DUMP_MD5_URI_PATTERN.format(wikidb, day),
                                  DUMP_BZ2_FILE_PATTERN.format(wikidb, day))
    to_download_files_info = prepare(hdfs_client, hdfs_path, files_info,
                                     no_check, force)

    hdfs_downloader = HDFSDownloader(name_node, user, num_threads, num_tries,
                                     buffer_size, timeout, force)
    hdfs_downloader.set_logging_level(logger.level)

    for (filename, md5sum) in to_download_files_info:
        url = DOWNLOAD_FILE_PATTERN.format(wikidb, day, filename)
        path = os.path.join(hdfs_path, filename)
        if no_check:
            hdfs_downloader.enqueue(url, path, None, None)
        else:
            hdfs_downloader.enqueue(url, path, "md5", md5sum)

    hdfs_downloader.start()
    hdfs_downloader.wait()

    if hdfs_downloader.tasks_with_errors():
        raise RuntimeError("Errors downloading some files:\n\t{0}".format(
            "\n\t".join(e[0] for e in hdfs_downloader.tasks_with_errors())))

    write_flag(hdfs_client, hdfs_path, flag)

    logger.debug("Done !")


def dump_completed(url):
    logger.debug("Checking for dump completion at {0}".format(url))
    req = requests.get(url)
    return ((req.status_code == 200) and ('Dump complete' in req.text))


def dump_files_infos(url, pattern):
    logger.debug("Getting files info to download from {0}".format(url))
    req = requests.get(url)
    files_info = []
    if (req.status_code == 200):
        p = re.compile(pattern)
        for line in req.text.split('\n'):
            match = p.search(line)
            if match:
                files_info.append((match.group(2), match.group(1)))
    return files_info


def prepare(hdfs_client, hdfs_path, files_info, no_check, force):
    """
    If hdfs_path exists on hdfs:
      - if force, delete folder and recreate it, return unchanged work list.
      - else check exisiting files md5 and update the work list with correct
          files, delete incorrect files, return updated work list.
    else:
      - create hdfs, return unchanged work list.
    Raise RuntimeError in case of error.
    """
    if hdfs_client.content(hdfs_path, strict=False):
        # Output path already exists
        if force:
            logger.debug("Deleting and recreating folder before download")
            try:
                hdfs_client.delete(hdfs_path, recursive=True)
                hdfs_client.makedirs(hdfs_path)
                return files_info
            except hdfs.HdfsError as e:
                logger.error(e)
                raise RuntimeError("Problem preparing for download [force].")
        else:
            logger.debug("Checking existing files in folder before download")
            try:
                return merge_files_info(hdfs_client, hdfs_path, files_info,
                                        no_check)
            except hdfs.HdfsError as e:
                logger.error(e)
                raise RuntimeError("Problem preparing for download [merge].")
    else:
        logger.debug("Creating folder before download")
        try:
            hdfs_client.makedirs(hdfs_path)
            return files_info
        except hdfs.HdfsError as e:
            logger.error(e)
            raise RuntimeError("Problem preparing for download [new].")


def merge_files_info(hdfs_client, hdfs_path, files_info, no_check):
    files_info_dic = dict(files_info)
    for filename in hdfs_client.list(hdfs_path):
        file_path = os.path.join(hdfs_path, filename)
        if filename not in files_info_dic:
            logger.debug("Deleting file not in dump from folder {0}".format(
                file_path))
            hdfs_client.delete(file_path, recursive=True)
        elif not (no_check and check_hdfs_md5(hdfs_client, file_path,
                                              files_info_dic[filename])):
            logger.debug("Deleting incorrect file from folder {0}".format(
                file_path))
            hdfs_client.delete(file_path, recursive=True)
        else:
            logger.debug("Correct file existing in folder, removing from " +
                         "download list ({0})".format(filename))
            del files_info_dic[filename]
    return files_info_dic.items()


def check_hdfs_md5(hdfs_client, file_path, md5sum):
    md5 = hashlib.md5()
    logger.debug("Checking md5sum for file {0}".format(file_path))
    with hdfs_client.read(file_path, chunk_size=4096) as reader:
        for chunk in reader:
            md5.update(chunk)
    return (md5.hexdigest() == md5sum)


def write_flag(hdfs_client, hdfs_path, flag):
    try:
        with hdfs_client.write(os.path.join(hdfs_path, flag)) as writer:
            pass
    except hdfs.HdfsError as e:
        logger.error(e)
        raise RuntimeError("Problem writing flag {0} in folder {1}".format(
            flag, hdfs_path))


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
