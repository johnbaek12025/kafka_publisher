import json
import logging
import socket
import time

from kpm.kafka_manager import KafkaPub

logger = logging.getLogger(__name__)


class Publisher(object):
    def __init__(self, **kafka_opts):
        self.stop_requested = False
        self.use_db = True
        self.kafka_mgr = None
        self.db_mgrs = {"rc_db": None}
        self.kafka_info = kafka_opts

    def run(self):
        self.connect_to_broker()
        try:
            while not self.stop_requested:
                rows = self.get_data_to_handle()
                for row in rows:
                    news_code = row.get("news_code")
                    news_sn = row.get("news_sn")
                    d_news_crt = row.get("d_news_crt")
                    topic = row.get("topic")

                    # broker 에 전송여부 상관없이, 읽어온 데이터는 무조서 성공처리
                    self.change_requests_status(
                        sn=news_sn, d_crt=d_news_crt, news_code=news_code
                    )

                    if None in [news_code, news_sn, d_news_crt]:
                        logger.info(
                            f"Please check message: news_code: {news_code}, news_sn: {news_sn}, d_news_crt: {d_news_crt}"
                        )
                        continue

                    message = {
                        "news_code": news_code,
                        "news_sn": news_sn,
                        "d_news_crt": d_news_crt,
                    }

                    self.kafka_mgr.send_messages([(topic, message, 0.01)])
                time.sleep(10)
        except (socket.error, socket.timeout, socket.herror) as e:
            logger.error(f"socket failure: {e}")
        except KeyboardInterrupt:
            pass
        finally:
            self.disconnect_from_broker()

    def connect_to_broker(self):
        logger.info("connect to kafka")
        self.kafka_mgr = KafkaPub()
        self.kafka_mgr.connect(**self.kafka_info)

    def disconnect_from_broker(self):
        if self.kafka_mgr:
            self.kafka_mgr.disconnect()

    def change_requests_status(self, sn, d_crt, news_code, status="S", commit=True):
        sql = f"""
            UPDATE  NEWS_USER.RTBL_COM_ADM_SEND A
            SET     A.STATUS = '{status}'
            WHERE   A.SN = {sn}
            AND     A.D_CRT = '{d_crt}'
            AND     A.NEWS_CODE = '{news_code}'
        """
        self.db_mgrs["rc_db"].modify(sql, commit)

    def get_data_to_handle(self):
        sql = f"""
            SELECT  A.SN, A.D_CRT, A.NEWS_CODE, B.CNL_CODE
            FROM    NEWS_USER.RTBL_COM_ADM_SEND A
                    , RTBL_LUP_NPC_NEW B
            WHERE   A.STATUS = 'N'
            AND     A.NEWS_CODE = B.NEWS_CODE
            AND     B.CNL_CODE IS NOT Null
        """
        rows = self.db_mgrs["rc_db"].get_all_rows(sql)
        x = []
        if not rows:
            return x
        for r in rows:
            row = {
                "news_sn": r[0],
                "d_news_crt": r[1],
                "news_code": r[2],
                "topic": r[3],
            }
            x.append(row)
        return x
