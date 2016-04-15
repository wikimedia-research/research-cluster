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
    --download-dump=<d>      Dump type to download, can be 'history' for
                              historical edits or 'current' for current version
                              [default: history]
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
from requests.packages.urllib3.exceptions import InsecurePlatformWarning

import hashlib
import re

import Queue
import threading

from hdfs_downloader import HDFSDownloader


requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
logger = logging.getLogger(__name__)


BASE_DUMP_URI_PATTERN = "https://dumps.wikimedia.org/{0}/{1}"
DUMP_STATUS_URI_PATTERN = BASE_DUMP_URI_PATTERN + "/status.html"
DUMP_SHA1_URI_PATTERN = BASE_DUMP_URI_PATTERN + "/{0}-{1}-sha1sums.txt"
DUMP_MD5_URI_PATTERN = BASE_DUMP_URI_PATTERN + "/{0}-{1}-md5sums.txt"
DUMP_HISTORY_BZ2_FILE_PATTERN = (
    "(\w{{32}})  ({0}-{1}-pages-meta-history.*\.xml.*\.bz2)")
DUMP_CURRENT_BZ2_FILE_PATTERN = (
    "(\w{{32}})  ({0}-{1}-pages-articles.*\.xml.*\.bz2)")
DOWNLOAD_FILE_PATTERN = BASE_DUMP_URI_PATTERN + "/{2}"

DUMP_TYPE_HISTORY = "history"
DUMP_TYPE_CURRENT = "current"

FILE_PRESENT = 0
FILE_ABSENT = 1
FILE_CORRUPT = 2


class DumpDownloader(object):

    def __init__(self,
                 wikidb,
                 day,
                 hdfs_path,
                 name_node,
                 user,
                 dump_type,
                 success_flag,
                 no_check,
                 num_threads,
                 num_tries,
                 buffer_size,
                 timeout,
                 force):
        self.wikidb = wikidb
        self.day = day
        self.hdfs_path = hdfs_path
        self.name_node = name_node
        self.user = user
        self.dump_type = dump_type
        self.succes_flag = success_flag
        self.no_check = no_check
        self.num_threads = num_threads
        self.num_tries = num_tries
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.force = force

        self._check_day_format()
        self._init_md5_url_pattern()

    #
    # Init Funtions
    #
    def _check_day_format(self):
        logger.debug("Checking day parameter format and values")
        p = re.compile("(\d{4})(\d\d)(\d\d)")
        match = p.search(self.day)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            if (year > 2010
               and month > 0 and month < 13
               and day > 0 and day < 32
               and not (month == 2 and day > 29)
               and not (month in [4, 6, 9, 11] and day > 30)):
                return
        raise RuntimeError("Wrong day parameter, expected yyyMMdd " +
                           "with valid values.")

    def _init_md5_url_pattern(self):
        logger.debug("Checking dump type parameter")
        if self.dump_type == DUMP_TYPE_HISTORY:
            self.md5_url_pattern = DUMP_HISTORY_BZ2_FILE_PATTERN.format(
                self.wikidb, self.day)
        elif self.dump_type == DUMP_TYPE_CURRENT:
            self.md5_url_pattern = DUMP_CURRENT_BZ2_FILE_PATTERN.format(
                self.wikidb, self.day)
        else:
            raise RuntimeError("Wrong dump type provided: {0}".format(
                self.dump_type))

    #
    # Run Funtions
    #
    def run(self):
        self._verify_dump_ready_for_download()
        self._configure_hdfs_client()
        self._identify_target_file_list_and_md5s()
        self._check_status_of_existing_files()
        self._prepare_hdfs()
        self._download_dumps()
        self._write_success_flag()

    def _verify_dump_ready_for_download(self):
        url = DUMP_STATUS_URI_PATTERN.format(self.wikidb, self.day)
        logger.debug("Checking for dump completion at {0}".format(url))
        req = requests.get(url)
        if not ((req.status_code == 200) and ('Dump complete' in req.text)):
            raise RuntimeError("Dump not ready to be downloaded")

    def _configure_hdfs_client(self):
        name_node = self.name_node
        user = self.user
        self.hdfs_client = hdfs.client.InsecureClient(name_node, user=user)

    def _identify_target_file_list_and_md5s(self):
        url = DUMP_MD5_URI_PATTERN.format(self.wikidb, self.day)
        logger.debug("Getting files list to download {0}".format(url))
        req = requests.get(url)
        self.filenames = []
        self.md5s = {}
        if (req.status_code == 200):
            p = re.compile(self.md5_url_pattern)
            for line in req.text.split('\n'):
                match = p.search(line)
                if match:
                    md5, filename = match.group(1), match.group(2)
                    self.filenames.append(filename)
                    self.md5s[filename] = md5
        else:
            raise RuntimeError("MD5 hash listing unavailable")

    def _check_status_of_existing_files(self):
        self.statuses = {}
        present_files = []
        logger.debug("Checking status of existing files")
        if self.hdfs_client.content(self.hdfs_path, strict=False):
            present_files = self.hdfs_client.list(self.hdfs_path)
        for filename in self.filenames:
            fullpath = os.path.join(self.hdfs_path, filename)
            if filename not in present_files:
                logger.debug("{0} is absent".format(filename))
                self.statuses[filename] = FILE_ABSENT
            elif self._confirm_checksum(filename):
                logger.debug("{0} is present".format(filename))
                self.statuses[filename] = FILE_PRESENT
            else:
                logger.debug("{0} is corrupted".format(filename))
                self.statuses[filename] = FILE_CORRUPT

    def _confirm_checksum(self, filename):
        logger.debug("confirming checksum for {0}".format(filename))
        found = self._md5sum_for_file(filename)
        given = self.md5s[filename]
        return given == found

    def _md5sum_for_file(self, filename):
        md5 = hashlib.md5()
        filepath = os.path.join(self.hdfs_path, filename)
        with self.hdfs_client.read(filepath, chunk_size=4096) as reader:
            for chunk in reader:
                md5.update(chunk)
        return md5.hexdigest()

    def _prepare_hdfs(self):
        logger.debug("Preparing HDFS for download")
        if self.hdfs_client.content(self.hdfs_path, strict=False):
            if self.force:
                try:
                    self.hdfs_client.delete(self.hdfs_path, recursive=True)
                    self.hdfs_client.makedirs(self.hdfs_path)
                    for f in self.statuses:
                        self.statuses[f] = FILE_ABSENT
                except hdfs.HdfsError as e:
                    logger.error(e)
                    raise RuntimeError("Problem preparing HDFS [force].")
            else:
                logger.debug("Preparing existing files before download")
                try:
                    self._remove_corrupt_and_unexpected_files()
                except hdfs.HdfsError as e:
                    logger.error(e)
                    raise RuntimeError("Problem preparing HDFS [merge].")
        else:
            try:
                self.hdfs_client.makedirs(self.hdfs_path)
            except hdfs.HdfsError as e:
                logger.error(e)
                raise RuntimeError("Problem preparing for HDFS [new].")

    def _remove_corrupt_and_unexpected_files(self):
        present_files = self.hdfs_client.list(self.hdfs_path)
        for filename in present_files:
            file_path = os.path.join(self.hdfs_path, filename)
            if (filename not in self.filenames):
                logger.debug("Deleting {0} because it doesn't belong".format(
                             filename))
                self.hdfs_client.delete(file_path, recursive=True)
            elif (self.statuses[filename] == FILE_CORRUPT):
                logger.debug("Deleting {0} because it is corrupted".format(
                             filename))
                self.hdfs_client.delete(file_path, recursive=True)
                self.statuses[filename] = FILE_ABSENT

    def _download_dumps(self):
        logger.debug("Instantiating and initialising  HDFSDownloader")
        hdfs_downloader = HDFSDownloader(self.name_node,
                                         self.user,
                                         HDFSDownloader.MD5_CHECK,
                                         self.num_threads,
                                         self.num_tries,
                                         self.buffer_size,
                                         self.timeout)
        hdfs_downloader.set_logging_level(logger.level)

        files_to_download = [f for f in self.statuses
                             if self.statuses[f] != FILE_PRESENT]
        for filename in files_to_download:
            file_url = DOWNLOAD_FILE_PATTERN.format(self.wikidb,
                                                    self.day,
                                                    filename)
            hdfs_file_path = os.path.join(self.output_path, filename)
            if self.no_check:
                # No check value needed
                hdfs_downloader.enqueue(url, path, None)
            else:
                file_md5 = self.md5s[filename]
                hdfs_downloader.enqueue((file_url, hdfs_file_path, file_md5))

        logger.debug("Starting to download")
        hdfs_downloader.start()
        hdfs_downloader.wait()

        if hdfs_downloader.tasks_with_errors():
            urls_in_error = "\n\t".join(err[0] for err in
                                        hdfs_downloader.tasks_with_errors())
            raise RuntimeError("Errors downloading files:\n\t{0}".format(
                urls_in_error))

    def _write_success_flag(self):
        success_flag_path = os.path.join(self.hdfs_path, self.success_flag)
        logger.debug("Writing success flag {0}".format(success_flag_path))
        try:
            with self.hdfs_client.write(success_flag_path) as writer:
                pass
        except hdfs.HdfsError as e:
            logger.error(e)
            raise RuntimeError("Problem writing success flag {0}".format(
                success_flag_path))


def main(args):
    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(name)s -- %(message)s'
    )
    logger.setLevel(logging.DEBUG if args['--debug'] else logging.INFO)

    wikidb = args["<wikidb>"]
    day = args["<day>"]
    hdfs_path = args["<hdfs-path>"]

    dump_type = args["--download-dump"]
    name_node = args["--name-node"]
    user = args["--user"]
    success_flag = args["--download-flag"]
    no_check = args["--download-no-check"]
    num_threads = int(args["--download-threads"])
    num_tries = int(args["--download-tries"])
    buffer_size = int(args["--download-buffer"])
    timeout = int(args["--download-timeout"])
    force = args["--force"]

    dl = DumpDownloader(
        wikidb,
        day,
        hdfs_path,
        name_node,
        user,
        dump_type,
        success_flag,
        no_check,
        num_threads,
        num_tries,
        buffer_size,
        timeout,
        force)
    dl.run()


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
