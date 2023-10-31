import mysql.connector as db
import time

conf = {
    'HOST': 'localhost',
    'PORT': '3307',
    'USER': 'manager',
    'PASSWORD': 'manager',
    'DATABASE': 'store',
}

def query(f):
    def query_(*args, **kwargs):
        conn = db.connect(
            host=conf["HOST"],
            port=conf["PORT"],
            user=conf["USER"],
            password=conf["PASSWORD"],
            db=conf["DATABASE"])
        try:
            result = f(*args, c=conn, **kwargs)
        except:
            conn.rollback()
            print("SQL failed")
            raise
        finally:
            conn.close()
        return result
    return query_


def invoke(f):
    def invoke_(*args, **kwargs):
        conn = db.connect(
            host=conf["HOST"],
            port=conf["PORT"],
            user=conf["USER"],
            password=conf["PASSWORD"],
            db=conf["DATABASE"])
        try:
            result = f(*args, c=conn, **kwargs)
        except:
            conn.rollback()
            print("SQL failed")
            raise
        else:
            conn.commit()
        finally:
            conn.close()
        return result
    return invoke_

class Db():
    def __init__(self, w_id=0):
        self.w_id = w_id

    @query
    def single(*args, query, d=True, **kwargs):
        conn = kwargs.pop("c")
        curr = conn.cursor(dictionary=d)
        curr.execute(query)
        return curr.fetchone()

    @query
    def select(*args, query, d=False, **kwargs):
        conn = kwargs.pop("c")

        curr = conn.cursor(dictionary=d)
        curr.execute(query)
        return curr.fetchall()
  
    @invoke
    def insert(*args, query, ex=True, **kwargs):
        conn = kwargs.pop("c")
        curr = conn.cursor()
        curr.execute(query)
        return (curr.statement, curr.rowcount)

    @invoke
    def insert_batch(*args, query, data, **kwargs):
        conn = kwargs.pop("c")
        curr = conn.cursor()
        curr.executemany(query, data)
        return curr.rowcount

    @invoke
    def update(*args, query, **kwargs):
        conn = kwargs.pop("c")
        curr = conn.cursor()
        curr.execute(query)
        return (curr.statement, curr.rowcount)

    @invoke
    def delete(*args, query, **kwargs):
        conn = kwargs.pop("c")
        curr = conn.cursor()
        curr.execute(query)
        return (curr.statement, curr.rowcount)
