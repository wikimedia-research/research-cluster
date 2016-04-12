"""
Reusable parallel download-to-HDFS script
"""
import logging
import os.path

import hdfs
import requests

import Queue
import threading


class HDFSDownloader(object):

    def __init__(self, name_node, user, num_threads, num_retries, buffer_size,
                 timeout, force):
        self.logger = logging.getLogger(__name__)
        self.name_node = name_node
        self.user = user
        self.num_threads = num_threads
        self.num_retries = num_retries
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.force = force

        self._q = Queue.Queue()
        self._errs = []

        self._threads = [threading.Thread(target=self._worker)
                         for _i in range(self.num_threads)]

    def set_logging_level(self, level):
        self.logger.setLevel(level)

    def enqueue(self, url, path):
        self.logger.debug("Enqueuing new url to download (total: {0})".format(
            self._q.qsize() + 1))
        self._q.put((url, path, ))

    def download(self):
        self.logger.debug("Starting {0} workers".format(self.num_threads))
        for thread in self._threads:
            thread.start()
            self._q.put((None, None))  # one EOF marker for each thread
        self._q.join()

        if self._errs:
            raise RuntimeError("Failed to download some file(s):\n\t" +
                               "{0}".format("\n\t".join(errs)))

    def _worker(self):
        thread_name = threading.current_thread().name
        hdfs_client = hdfs.client.InsecureClient(self.name_node,
                                                 user=self.user)
        self.logger.debug("Starting worker {0}".format(thread_name))
        while True:
            (url, path) = self._q.get()
            if url is None:  # EOF?
                self._q.task_done()
                self.logger.debug("Received EOF, stopping worker {0}".format(
                    thread_name))
                return
            if (not self._download_to_hdfs(hdfs_client, url, path)):
                self._errs.append(url)
                self.logger.warn("Unsuccessful task for worker {0}".format(
                    thread_name))
            else:
                self.logger.debug("Successful task for worker {0}".format(
                    thread_name))
            self._q.task_done()

    def _download_to_hdfs(self, hdfs_client, url, path):
        session = requests.Session()
        session.mount("http://", requests.adapters.HTTPAdapter(
            max_retries=self.num_retries))
        req = session.get(url, stream=True, timeout=self.timeout)
        self.logger.debug("Downloading from {0} ".format(url) +
                          "and uploading to {0} ".format(path))
        try:
            hdfs_client.write(path, data=req.iter_content(self.buffer_size),
                              buffersize=self.buffer_size, overwrite=True)
            return True
        except Exception as e:
            logger.debug("Error while downloading {0}: {1}".format(
                url, str(e)))
            return False
