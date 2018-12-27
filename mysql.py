import pymysql
import logging

from database import Database

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Mysql(Database):
    def __init__(self, **kwargs):
        host = kwargs['host']
        user = kwargs['username']
        passwd = kwargs['password']
        self.db = kwargs['dbname']

        self.conn = pymysql.connect(
                        host=host,
                        user=user,
                        passwd=passwd,
                        db=self.db,
                        cursorclass=pymysql.cursors.DictCursor
                    )

        self.version = self.get_version()

    def get_version(self):
        query = 'SHOW VARIABLES LIKE "version"'
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            version = cursor.fetchone().get('Value')

        if version:
            return version.rsplit('.', 1)[0]

        return None

    def create_user(self, user, passw):
        query = 'CREATE USER `{}`@"%" IDENTIFIED BY "{}"'
        
        if self.version != '5.6':
            query = query + 'PASSWORD EXPIRE INTERVAL 1 DAY'
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query.format(user, passw))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error("%s: %s", e, e.args[0])
            return False

        return True

    def get_new_password(self, user, passw):
        query = 'ALTER USER `{}` IDENTIFIED BY "{}" PASSWORD EXPIRE INTERVAL 1 DAY'

        if self.version == '5.6':
            query = 'SET PASSWORD FOR "{}"@"%" = PASSWORD("{}")'
            
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query.format(user, passw))

            self.conn.commit()

        except Exception as e:
            logger.error("%s: %s", e, e.args[0])
            return False

        return True