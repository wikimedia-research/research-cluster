"""
Reusable parallel download-to-HDFS script
"""
import logging
import os.path

import hdfs
import requests
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
import hashlib

import multiprocessing
import Queue


requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
logger = logging.getLogger(__name__)


class HDFSDownloader(object):

    # Reusable constants
    SHA1_CHECK = "sha1"
    MD5_CHECK = "md5"
    SIZE_CHECK = "size"

    def __init__(self,
                 name_node,
                 user,
                 check,
                 num_checkers,
                 num_downloaders,
                 num_tries,
                 buffer_size,
                 timeout,
                 debug):

        self.name_node = name_node
        self.user = user
        self.check = check
        self.num_checkers = num_checkers
        self.num_downloaders = num_downloaders
        self.num_tries = num_tries
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.debug = debug

        self._dwq = multiprocessing.Queue()
        self._downloaders = [multiprocessing.Process(target=self._downloader)
                             for _i in range(self.num_downloaders)]

        self._ckq = multiprocessing.JoinableQueue()
        self._checkers = [multiprocessing.Process(target=self._checker)
                          for _i in range(self.num_checkers)]

        self._errq = multiprocessing.Queue()

        self._init_logging()

    def _init_logging(self):
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(name)s -- %(message)s'
        )
        logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

    def enqueue(self, url, path, check_val):
        """
        Add dowload tasks to the downloader.
        """
        logger.debug("Enqueuing new task (total: {0})".format(
            self._dwq.qsize() + 1))
        self._dwq.put((url, path, check_val, 1))

    def start(self):
        """
        Starts the download & check processes
        """
        logger.debug("Starting {0} downloaders".format(self.num_downloaders))
        for p in self._downloaders:
            # p.daemon = True
            p.start()
        logger.debug("Starting {0} checkers".format(self.num_checkers))
        for p in self._checkers:
            # p.daemon = True
            p.start()

    def wait(self):
        """
        Blocking method waiting for all threads to be finished.
        """
        [p.join() for p in self._downloaders]
        self._ckq.join()
        [p.terminate() for p in self._checkers]
        [p.join() for p in self._checkers]

    def tasks_with_errors(self):
        """
        Returns the list of tasks (url, path, check_val)
        having encountered error at download or check
        """
        errs = []
        while True:
            try:
                errs.append(self._errq.get_nowait())
            except Queue.Empty:
                break
        return errs

    def _downloader(self):
        my_name = multiprocessing.current_process().name
        hdfs_client = hdfs.client.InsecureClient(self.name_node,
                                                 user=self.user)
        logger.debug("Starting downloader {0}".format(my_name))

        while True:
            try:
                (url, path, check_val, current_try) = self._dwq.get_nowait()
            except Queue.Empty:
                logger.debug("Stopping downloader {0}".format(my_name))
                return
            # Successful download, add task for checking
            if self._download_to_hdfs(hdfs_client, url, path):
                logger.debug("Successful download by {0} ".format(my_name) +
                             "({0} downloads left)".format(
                                self._dwq.qsize() + 1))
                self._ckq.put((url, path, check_val, current_try))
            # Failed download, re-enqueue for new try
            elif current_try < self.num_tries or self.num_tries == 0:
                logger.warn("Failed download by {0},".format(my_name) +
                            "re-enqueuing (try {0})".format(current_try + 1))
                self._dwq.put((url, path, check_val, current_try + 1))
            # Failed download, no new try, add to errors
            else:
                logger.warn("Failed download by {0} ".format(my_name) +
                            "max retries reached {0} ".format(self.num_tries) +
                            "logging task as error ({0} downoads left)".format(
                                self._dwq.qsize() + 1))
                self._errq.put((url, path, check_val))

    def _download_to_hdfs(self, hdfs_client, url, path):
        session = requests.Session()
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=3))
        req = session.get(url, stream=True, timeout=self.timeout)

        # Dowload file  and stream it into hdfs
        logger.debug("Downloading from {0} and uploading to {1}".format(
            url, path))
        try:
            hdfs_client.write(path, data=req.iter_content(self.buffer_size),
                              buffersize=self.buffer_size, overwrite=True)
            return True
        except Exception as e:
            logger.debug("Error while downloading {0}: {1}".format(
                url, str(e)))
            return False

    def _checker(self):
        my_name = multiprocessing.current_process().name
        hdfs_client = hdfs.client.InsecureClient(self.name_node,
                                                 user=self.user)
        logger.debug("Starting checker {0}".format(my_name))

        while True:
            (url, path, check_val, current_try) = self._ckq.get()
            retry = current_try < self.num_tries or self.num_tries == 0
            try:
                if not self._check(hdfs_client, path, check_val):
                    if retry:
                        logger.debug("Unsuccessful check by {0} ".format(
                            my_name) + "downloading again (try {0})".format(
                            current_try + 1))
                        self._dwq.put((url, path, check_val, current_try + 1))
                    else:
                        logger.warn("Failed check by {0} ".format(
                            my_name) + "max retries reached {0} ".format(
                            self.num_tries) + "logging task as error " +
                            "({0} checks left)".format(self._ckq.qsize() + 1))
                        self._errq.put((url, path, check_val))
            except RetryCheckException:
                if retry:
                    logger.warn("Failed check by {0},".format(
                        my_name) + "re-checking (try {0})".format(
                        current_try + 1))
                    self._ckq.put((url, path, check_val, current_try + 1))
                else:
                    logger.warn("Failed check by {0} ".format(
                        my_name) + "max retries reached {0} ".format(
                        self.num_tries) + "logging task as error " +
                        "({0} checks left)".format(self._ckq.qsize() + 1))
                    self._errq.put((url, path, check_val))
            except Exception:
                logger.warn("Failed check by {0} ".format(
                    my_name) + "max retries reached {0} ".format(
                    self.num_tries) + "logging task as error " +
                    "({0} checks left)".format(self._ckq.qsize() + 1))
                self._errq.put((url, path, check_val))

            self._ckq.task_done()

    def _check(self, hdfs_client, path, check_val):
        # Checking checks settings
        if not self.check:
            logger.debug("No check given, not checking {0} ".format(path))
            return True

        if not check_val:
            logger.warn("Empty check value for file {0}".format(path))
            raise RuntimeError("Empty value for checker")

        if self.check == self.SHA1_CHECK:
            hasher = hashlib.sha1()
            return self._check_crypto(hdfs_client, path, check_val, hasher)
        elif self.check == self.MD5_CHECK:
            hasher = hashlib.md5()
            return self._check_crypto(hdfs_client, path, check_val, hasher)
        elif self.check == self.SIZE_CHECK:
            return self._check_size(hdfs_client, path, check_val)
        else:
            logger.warn("Wrong check method ({0})".format(check, path))
            raise RuntimeError("Undefined check method")

    def _check_crypto(self, hdfs_client, path, check_val, hasher):
        logger.debug("Checking crypto validity for {0}".format(path))
        try:
            with hdfs_client.read(path, chunk_size=4096) as reader:
                for chunk in reader:
                    hasher.update(chunk)
            return (hasher.hexdigest() == check_val)
        except Exception as e:
            logger.debug("Error while crypto checking {0}: {1}".format(
                path, str(e)))
            raise RetryCheckException()

    def _check_size(self, hdfs_client, path, check_val):
        logger.debug("Checking size validity for {0}".format(path))
        try:
            return hdfs_client.status(path, strict=True)["length"] == check_val
        except Exception as e:
            logger.debug("Error while size checking {0}: {1}".format(
                path, str(e)))
            raise RetryCheckException()


class RetryCheckException(Exception):
    pass
