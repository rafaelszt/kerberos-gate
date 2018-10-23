import pymysql

from database import Database


class Mysql(Database):
    def __init__(self, **kwargs):
        host = kwargs['host']
        user = kwargs['username']
        passwd = kwargs['password']
        db = kwargs['dbname']

        self.conn = pymysql.connect(
                        host=host,
                        user=user,
                        passwd=passwd,
                        db=db,
                        cursorclass=pymysql.cursors.DictCursor
                    )

    def create_user(self, user, passw):
        query = 'CREATE USER `{}`@"%" IDENTIFIED BY "{}" PASSWORD EXPIRE INTERVAL 1 DAY'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query.format(user, passw))
            
            self.conn.commit()
            
        except Exception as e:
            return False

        return True

    def get_new_password(self, user, passw):
        query = 'ALTER USER `{}` IDENTIFIED BY "{}" PASSWORD EXPIRE INTERVAL 1 DAY'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query.format(user, passw))

            self.conn.commit()

        except Exception as e:
            return False

        return True