from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

from database import Database


class Postgresql(Database):
    def __init__(self, **kwargs):
        host = kwargs['host']
        user = kwargs['username']
        passwd = kwargs['password']
        db = kwargs['dbname']

        self.conn = psycopg2.connect(
                    host=host,
                    user=user,
                    password=passwd,
                    dbname=db,
                    cursor_factory=psycopg2.extras.DictCursor
                )

    def create_user(self, user, passw):
        query = "CREATE USER \"{}\" WITH PASSWORD `{}` VALID UNTIL `{}`"
        reset_time = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S -03")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query.format(user, passw, reset_time))
            
            self.conn.commit()
            
        except Exception:
            return False

        return True

    def get_new_password(self, user, passw):
        query = "ALTER USER \"{}\" WITH PASSWORD '{}' VALID UNTIL '{}'"
        reset_time = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S -03")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query.format(user, passw, reset_time))

            self.conn.commit()

        except Exception:
            return False

        return True