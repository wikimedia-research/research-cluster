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
        self.init()

    def init(self):
        if not self.done():
            return False
        self._q = Queue.Queue()
        self._errs = []
        self._threads = [threading.Thread(target=self._worker)
                         for _i in range(self.num_threads)]
        return True

    def set_logging_level(self, level):
        self.logger.setLevel(level)

    def enqueue(self, url, path, md5sum):
        """
        Add dowload tasks to the downloader.
        """
        self.logger.debug("Enqueuing new task (total: {0})".format(
            self._q.qsize() + 1))
        self._q.put((url, path, md5sum, 1))

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
        Returns the list of tasks (url, path, md5sum)
        having encountered error at download or checksum
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
                (url, path, md5sum, tries) = self._q.get()
                if not self._download_to_hdfs(hdfs_client, url, path, md5sum):
                    if (tries < self.num_tries or self.num_tries == 0):
                        self.logger.warn("Failed task in worker {0},".format(
                            thread_name) + " re-enqueuing (try {0})".format(
                            tries + 1))
                        self._q.put((url, path, md5sum, tries + 1))
                    else:
                        self.logger.warn("Failed task in worker {0}, ".format(
                            thread_name) + "max retried reach ({0}), ".format(
                            self.num_tries) + " logging task as error " +
                            "({0} tasks left)".format(self._q.qsize() + 1))
                        self._errs.append((url, path, md5sum))
                else:
                    self.logger.debug("Successful task for worker {0} ".format(
                        thread_name) + "({0} tasks left)".format(
                        self._q.qsize() + 1))
                self._q.task_done()
            except Queue.Empty:
                self.logger.debug("No more tasks, stopping worker {0}".format(
                    thread_name))
                return

    def _download_to_hdfs(self, hdfs_client, url, path, md5sum):
        req = requests.get(url, stream=True, timeout=self.timeout)

        # Dowload file  and stream it into hdfs
        self.logger.debug("Downloading from {0} ".format(url) +
                          "and uploading to {0} ".format(path))
        try:
            hdfs_client.write(path, data=req.iter_content(self.buffer_size),
                              buffersize=self.buffer_size, overwrite=True)
        except Exception as e:
            self.logger.debug("Error while downloading {0}: {1}".format(
                url, str(e)))
            return False

        if not md5sum:
            self.logger.debug("No md5 given, not checking {0} ".format(path))
            return True

        # Check md5 of hdfs file
        self.logger.debug("Checking md5 validity for {0} ".format(path))
        try:
            md5 = hashlib.md5()
            with hdfs_client.read(path, chunk_size=4096) as reader:
                for chunk in reader:
                    md5.update(chunk)
            return (md5.hexdigest() == md5sum)
        except Exception as e:
            self.logger.debug("Error while checking md5 for {0}: {1}".format(
                path, str(e)))
            return False
