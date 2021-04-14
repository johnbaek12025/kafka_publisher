"""
- ctrl+c dose not work with DISABLE_OOB = ON in sqlnet.ora
- run the below code as a makeshift
  $ kill -9 `ps -ef | grep "python bin/ks-manager.py" | grep -v grep | awk '{print $2}'` 
"""


import logging
import time

from kpm.db_manager import DBManager
from kpm.process_manager import ProcessorProcess, ProcessorThread, finalize_processor

logger = logging.getLogger(__name__)


class KPException(Exception):
    def __init__(self, error_msg):
        super().__init__(error_msg)


class KPManager(object):
    class Terminate(Exception):
        """Exception for terminating the application"""

    @staticmethod
    def sigTERMHandler(signum, dummy_frame):
        raise KPManager.Terminate

    def __init__(self):
        self.kafka_manager = None
        # fixme: 아래 count 를 사용하기 위해서, kafka의 group과 partition 설정해야함
        self.publishers = [
            {"pub_id": "admin_adapter", "count": 1},
        ]
        self.workers = {}

    @staticmethod
    def get_analysis_object(pub_id, **kafka_opts):
        try:
            exec(f"from .pubs.{pub_id} import Publisher")
        except ModuleNotFoundError as err:
            logger.info(f"Publisher {pub_id}는 존재하지 않습니다: {err}")
            return None
        c = eval("Publisher(**kafka_opts)")
        return c

    def initialize(self, config_dict, use_processes=True):
        kafka_opts = config_dict.get("kafka", {})

        for s in self.publishers:
            pub_id = s["pub_id"]
            count = s["count"]
            proc = KPManager.get_analysis_object(pub_id, **kafka_opts)

            for i in range(count):
                pub_proc_id = f"{pub_id}_{i}"
                if proc.use_db:
                    try:
                        for db_id in proc.db_mgrs:
                            db_info = config_dict.get(db_id, {})
                            proc.db_mgrs[db_id] = KPManager.get_connected_mgr(
                                pub_proc_id, db_info
                            )
                    except AttributeError as err:
                        logger.info(f"{pub_proc_id}: {err}")
                        continue

                self.workers[pub_proc_id] = self.create_child(
                    pub_proc_id, proc, use_processes
                )

    def create_child(self, label, proc, use_processes):
        if not proc:
            return
        if not use_processes:
            return ProcessorThread(proc, label)
        return ProcessorProcess(proc, label)

    def run(self):
        for p_id in self.workers:
            child = self.workers[p_id]
            if child:
                logger.info("start %s" % p_id)
                child.start()
            else:
                logger.info("cannot start %s" % p_id)

        while True:
            try:
                time.sleep(0.1)
            except (KeyboardInterrupt, KPManager.Terminate):
                print(f"KeyboardInterrupted")
                break
        self.stop()

    def stop(self):
        self.disconnect_from_db()
        finalize_processor(self.workers)
        logger.info("shutdown kafka publisher manager")

    def disconnect_from_db(self):
        for p_id in self.workers:
            child = self.workers[p_id]
            if child is None:
                continue
            for db_id in child.mgr.db_mgrs:
                db = child.mgr.db_mgrs[db_id]
                db.disconnect()

    @staticmethod
    def get_connected_mgr(label, db_info):
        db_mgr = None
        if db_info is None:
            return db_mgr
        db_mgr = DBManager()
        db_mgr.connect(**db_info)
        return db_mgr
