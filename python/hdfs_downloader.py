"""
Reusable parallel download-to-HDFS script
"""
import logging
import os.path

import hdfs
import requests
from requests.packages.urllib3.exceptions import InsecurePlatformWarning
import hashlib

import Queue
import threading


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
                 num_threads,
                 num_tries,
                 buffer_size,
                 timeout,
                 debug):

        self.name_node = name_node
        self.user = user
        self.check = check
        self.num_threads = num_threads
        self.num_tries = num_tries
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.debug = debug

        self._q = Queue.Queue()
        self._errs = []
        self._threads = [threading.Thread(target=self._worker)
                         for _i in range(self.num_threads)]

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
            self._q.qsize() + 1))
        self._q.put((url, path, check_val, 1))

    def start(self):
        """
        Starts the download threads
        """
        logger.debug("Starting {0} workers".format(self.num_threads))
        for thread in self._threads:
            thread.start()

    def wait(self):
        """
        Blocking method waiting for all tasks to be done.
        """
        self._q.join()

    def tasks_with_errors(self):
        """
        Returns the list of tasks (url, path, check_val)
        having encountered error at download or check
        """
        return self._errs

    def done(self):
        """
        Returns true if no download thread is still alive
        """
        return not any([t.isAlive() for t in self._threads])

    def _worker(self):
        thread_name = threading.current_thread().name
        hdfs_client = hdfs.client.InsecureClient(self.name_node,
                                                 user=self.user)
        logger.debug("Starting worker {0}".format(thread_name))

        while True:
            try:
                (url, path, check_val, current_try) = self._q.get()
            except Queue.Empty:
                logger.debug("No more tasks, stopping worker {0}".format(
                    thread_name))
                return

            downloaded = self._download_to_hdfs(hdfs_client, url, path)
            checked = downloaded and self._check(hdfs_client, path, check_val)

            if not (downloaded and checked):

                retry = current_try < self.num_tries or self.num_tries == 0

                if retry:
                    logger.warn("Failed task in worker {0},".format(
                        thread_name) + " re-enqueuing (try {0})".format(
                        tries + 1))

                    self._q.put((url, path, check_val, tries + 1))
                else:
                    logger.warn("Failed task in worker {0}, ".format(
                        thread_name) + "max retried reach ({0}), ".format(
                        self.num_tries) + " logging task as error " +
                        "({0} tasks left)".format(self._q.qsize() + 1))

                    self._errs.append((url, path, check_val))

            else:
                logger.debug("Successful task for worker {0} ".format(
                    thread_name) + "({0} tasks left)".format(
                    self._q.qsize() + 1))

            self._q.task_done()

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

    def _check(self, hdfs_client, path, check_val):
        # Checking checks settings
        if not self.check:
            logger.debug("No check given, not checking {0} ".format(path))
            return True

        if not check_val:
            logger.warn("Empty check value for file {0}".format(path))
            return False

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
            return False

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
            return False

    def _check_size(self, hdfs_client, path, check_val):
        logger.debug("Checking size validity for {0}".format(path))
        try:
            return hdfs_client.status(path, strict=True)["length"] == check_val
        except Exception as e:
            logger.debug("Error while size checking {0}: {1}".format(
                path, str(e)))
            return False
