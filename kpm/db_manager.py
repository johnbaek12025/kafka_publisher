import cx_Oracle as oracle
import logging

logger = logging.getLogger(__name__)


def mkdbstr(dbhost, dbport, dbname, dbuser=None):
    if dbuser is not None:
        return "%s@%s:%s/%s" % (dbuser, dbhost, dbport, dbname)
    return "%s:%s/%s" % (dbhost, dbport, dbname)


class DBManager(object):
    def __init__(self):
        self.uri = None
        self.conn = None

    def set_lib_path(self, lib_path):
        try:
            oracle.init_oracle_client(lib_dir=lib_path)
        except Exception as err:
            logger.info(f"set_lib_path: {err}")

    def connect(self, db_host, db_port, db_name, db_user, db_pass=None):
        self.uri = mkdbstr(db_host, db_port, db_name, db_user)
        logger.info(f"connect to {self.uri}")
        tns_name = oracle.makedsn(db_host, db_port, service_name=db_name)
        try:
            self.conn = oracle.connect(db_user, db_pass, tns_name)
        except Exception as err:
            logger.error(f"connection to {self.uri} failed : {err}")
            raise

    def disconnect(self):
        if self.conn:
            logger.info(f"disconnect from {self.uri}")
            self.conn.close()
            self.conn = None

    def callproc(self, procedure_name, args=()):
        try:
            with self.conn.cursor() as cur:
                logger.info(f"procedure: {procedure_name}, arguments: {args}")
                cur.callproc(procedure_name, args)
        except Exception as err:
            logger.error(f"procedure {procedure_name} failed: {err}")

    def get_all_rows(self, sql):
        try:
            cur = self.conn.cursor()
            logger.info("sql: %s" % sql)
            cur.execute(sql)
            return cur.fetchall()
        except Exception as err:
            logger.error(f"query {sql} failed: {err}")
            raise

    def modify_many(self, sql, rows, commit):
        try:
            cur = self.conn.cursor()
            logger.info("sql: %s" % sql)
            cur.prepare(sql)
            cur.executemany(None, rows)
            if commit:
                self.conn.commit()
        except Exception as err:
            logger.error(f"query {sql} failed: {err}")
            raise

    def modify(self, sql, commit):
        try:
            cur = self.conn.cursor()
            logger.info("sql: %s" % sql)
            cur.execute(sql)
            if commit:
                self.conn.commit()
        except Exception as err:
            logger.info(f"query {sql} failed: {err}")
            raise

    def commit(self):
        try:
            self.conn.commit()
        except Exception as err:
            logger.info(f"commit failed: {err}")
            raise