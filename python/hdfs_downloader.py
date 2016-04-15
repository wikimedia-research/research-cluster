"""
Reusable parallel download-to-HDFS script
"""
import logging
import os.path

import hdfs
import requests
import hashlib

import Queue
import threading


class HDFSDownloader(object):

    def __init__(self, name_node, user, num_threads, num_tries, buffer_size,
                 timeout, force):
        self.logger = logging.getLogger(__name__)
        self.name_node = name_node
        self.user = user
        self.num_threads = num_threads
        self.num_tries = num_tries
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.force = force
        self._q = Queue.Queue()
        self._errs = []
        self._threads = [threading.Thread(target=self._worker)
                         for _i in range(self.num_threads)]

    def set_logging_level(self, level):
        self.logger.setLevel(level)

    def enqueue(self, url, path, check, check_val):
        """
        Add dowload tasks to the downloader.
        """
        self.logger.debug("Enqueuing new task (total: {0})".format(
            self._q.qsize() + 1))
        self._q.put((url, path, check, check_val, 1))

    def start(self):
        """
        Starts the download threads
        """
        self.logger.debug("Starting {0} workers".format(self.num_threads))
        for thread in self._threads:
            thread.start()

    def wait(self):
        """
        Blocking method waiting for all tasks to be done.
        """
        self._q.join()

    def tasks_with_errors(self):
        """
        Returns the list of tasks (url, path, check, check_val)
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
        self.logger.debug("Starting worker {0}".format(thread_name))
        while True:
            try:
                (url, path, check, check_val, tries) = self._q.get()
                if not (self._download_to_hdfs(hdfs_client, url, path) and
                        self._check(hdfs_client, path, check, check_val)):
                    if (tries < self.num_tries or self.num_tries == 0):
                        self.logger.warn("Failed task in worker {0},".format(
                            thread_name) + " re-enqueuing (try {0})".format(
                            tries + 1))
                        self._q.put((url, path, check, check_val, tries + 1))
                    else:
                        self.logger.warn("Failed task in worker {0}, ".format(
                            thread_name) + "max retried reach ({0}), ".format(
                            self.num_tries) + " logging task as error " +
                            "({0} tasks left)".format(self._q.qsize() + 1))
                        self._errs.append((url, path, check, check_val))
                else:
                    self.logger.debug("Successful task for worker {0} ".format(
                        thread_name) + "({0} tasks left)".format(
                        self._q.qsize() + 1))
                self._q.task_done()
            except Queue.Empty:
                self.logger.debug("No more tasks, stopping worker {0}".format(
                    thread_name))
                return

    def _download_to_hdfs(self, hdfs_client, url, path, check, check_val):
        session = requests.Session()
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=3))
        req = session.get(url, stream=True, timeout=self.timeout)

        # Dowload file  and stream it into hdfs
        self.logger.debug("Downloading from {0} ".format(url) +
                          "and uploading to {0} ".format(path))
        try:
            hdfs_client.write(path, data=req.iter_content(self.buffer_size),
                              buffersize=self.buffer_size, overwrite=True)
            return True
        except Exception as e:
            self.logger.debug("Error while downloading {0}: {1}".format(
                url, str(e)))
            return False

    def _check(self, hdfs_client, url, path, check, check_val):
        # Checking checks settings
        if not check:
            self.logger.debug("No check given, not checking {0} ".format(path))
            return True

        if check == "md5":
            if not check_val:
                self.logger.debug("Empty md5 check value for file {0}".format(
                    path))
            return False

            self.logger.debug("Checking md5 validity for {0}".format(path))
            try:
                md5 = hashlib.md5()
                with hdfs_client.read(path, chunk_size=4096) as reader:
                    for chunk in reader:
                        md5.update(chunk)
                return (md5.hexdigest() == check)
            except Exception as e:
                self.logger.debug("Error while checking md5 for {0}".format(
                    path) + ": {0}".format(str(e)))
                return False

        elif check == "size":
            if not check_val:
                self.logger.debug("Empty size check value for file {0}".format(
                    path))
            return False

            self.logger.debug("Checking size validity for {0}".format(path))
            try:
                return hdfs_client.status(path, strict=True)["length"] == check
            except Exception as e:
                self.logger.debug("Error while checking size for {0}".format(
                    path) + ": {0}".format(str(e)))
                return False

        else:
            self.logger.debug("Wrong check method ({0}) for file {1} ".format(
                check, path))
            return False
