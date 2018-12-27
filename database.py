class Database:
    def __init__(self):
        raise NotImplementedError

    def create_user(self, conn, user, passw):
        raise NotImplementedError

    def get_new_password(self, user, passw):
        raise NotImplementedError

    def get_db_version(self):
        raise NotImplementedError