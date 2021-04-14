import logging

from kpm.kafka_manager import KafkaPub

logger = logging.getLogger(__name__)


class PublisherManager(object):
    def __init__(self):
        self.use_db = False
        self.kafka_mgr = None
        self.db_mgrs = {}

    def stop(self):
        self.disconnect_from_db()
        self.disconnect_from_kafka()

    def disconnect_from_db(self):
        if not bool(self.db_mgrs):
            return

        for mgr_id in self.db_mgrs:
            mgr = self.db_mgrs[mgr_id]
            if mgr is None:
                continue
            mgr.disconnect()

    def connect(self, kafka_info):
        logger.info("connect to kafka")
        self.kafka_mgr = KafkaPub()
        self.kafka_mgr.connect(**kafka_info)

    def disconnect_from_kafka(self):
        if self.kafka_mgr:
            self.kafka_mgr.disconnect()