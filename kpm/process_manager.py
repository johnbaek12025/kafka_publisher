"""
Library of components for thread and multi-process
"""

import os
import threading
import multiprocessing
import logging
import signal

logger = logging.getLogger(__name__)


def finalize_processor(workers):
    for p_id in workers:
        child = workers[p_id]
        if child is None:
            continue
        logger.info(f"stop process {p_id}")
        child.stop()

    for p_id in workers:
        child = workers[p_id]
        if child is None:
            continue
        logger.info(f"join {p_id}")
        child.terminate()
        child.join()


class ProcessorThread(threading.Thread):
    def __init__(self, mgr, name):
        threading.Thread.__init__(self)
        self.mgr = mgr
        self.name = name

    def run(self):
        self.mgr.run()

    def stop(self):
        self.mgr.stop_requested = True

    def terminate(self):
        # dummy function for API compatibility with multiprocessing
        pass


class ProcessorProcess(multiprocessing.Process):
    def __init__(self, mgr, name):
        multiprocessing.Process.__init__(self)
        self.mgr = mgr
        self.name = name

    def run(self):
        # we do not want children to respond to SIGINT - parent will reap them
        # signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        # signal.signal(signal.SIGINT, self.signal_handler)
        self.make_pid_list()
        self.mgr.run()

    def stop(self):
        self.mgr.stop_requested = True
        self.mgr.stop()
        self.signal_handler()

    def signal_handler(self):
        if self.pid:
            print(f"terminated process: {self.name}, pid: {self.pid}")
            kill_command = "kill -9 %s" % self.pid
            os.system(kill_command)

    def make_pid_list(self):
        print(f"create process {self.name}, {os.getpid()}")
