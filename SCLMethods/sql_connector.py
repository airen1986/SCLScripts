import apsw
import os, traceback, threading

connector = {}


class UserError(Exception):
    message: str

    def __init__(self, msg):
        self.message = msg


class SccCursor:
    def __init__(self, conn, db_path):
        self.conn = conn
        self.db_path = db_path

    def execute(self, query, args=tuple()):
        try:
            self.conn.execute(query, args)
        except Exception as ex:
            print(query)
            raise ex
        return self.conn

    def intermediate_commit(self):
        self.conn.execute("COMMIT")
        self.conn.execute("BEGIN")

    def get_db_path(self):
        return self.db_path


class SqlConnect:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.cursor = get_sql_cursor(db_path)

    def __enter__(self):
        try:
            self.cursor.execute("BEGIN")
        except apsw.SQLError:
            self.cursor.execute("ROLLBACK")
            self.cursor.execute("BEGIN")
        return SccCursor(self.cursor, self.db_path)

    def __exit__(self, exception_type, exception_value, traceback_val):
        if exception_type:
            print(f"some error happened {self.db_path} {exception_type} {exception_value} {str(traceback_val)}")
            traceback.print_exc()
            try:
                self.cursor.execute("ROLLBACK")
            except:
                pass
            self.cursor.close()
            raise UserError(str(exception_value))
        else:
            self.cursor.execute("COMMIT")


def init_db(db_path):
    if not os.path.isfile(db_path):
        raise UserError(f"DBFile {db_path} Doesn't exists in system")
    conn = apsw.Connection(db_path)
    conn.setbusytimeout(120000)
    conn.cursor().execute("PRAGMA temp_store =  MEMORY")
    return conn


def get_sql_cursor(db_path):
    tid = str(threading.get_ident())
    if db_path in connector and tid in connector[db_path]:
        connection = connector[db_path][tid]
        if connection:
            return connection.cursor()
    connection = init_db(db_path)
    if db_path in connector:
        connector[db_path][tid] = connection
    else:
        connector[db_path] = {tid: connection}
    return connection.cursor()


