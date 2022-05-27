import pymysql,json,logging
from dbutils.pooled_db import PooledDB
from .file import db_connection

MYSQL_CONFIG = {
    "host": "65.2.3.161",
    "port": 3306,
    "db": "d4c_fhir_datastore",
    "password": "fhir@123",
    "user": "fhir",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
}
POOL_CONFIG = {
    # Modules using linked databases
    "creator": pymysql,
    # Maximum connections allowed for connection pool,
    # 0 and None Indicates unlimited connections
    "maxconnections": 6,
    # At least idle links created in the link pool during initialization,
    # 0 means not to create
    "mincached": 2,
    # The most idle links in the link pool,
    # 0 and None No restriction
    "maxcached": 5,
    # The maximum number of links shared in the link pool,
    # 0 and None Represents all shares.
    # PS: It's useless because pymysql and MySQLdb Equal module threadsafety
    # All are 1, no matter how many values are set,_maxcached Always 0,
    # so always all links are shared.
    "maxshared": 3,
    # If there is no connection available in the connection pool,
    # whether to block waiting. True，Waiting;
    # False，Don't wait and report an error
    "blocking": True,
    # The maximum number of times a link is reused,
    # None Indicates unlimited
    "maxusage": None,
    # List of commands executed before starting a session.
    # Such as:["set datestyle to ...", "set time zone ..."]
    "setsession": [],
    # ping MySQL Server, check whether the service is available.
    # # For example: 0 = None = never,
    # 1 = default = whenever it is requested,
    # 2 = when a cursor is created,
    # 4 = when a query is executed,
    # 7 = always
    "ping": 0,
}
POOL = PooledDB(**MYSQL_CONFIG, **POOL_CONFIG)
class SqlPooled:
    def __init__(self):
        self._connection = POOL.connection()
        self._cursor = self._connection.cursor()

    def fetch_one(self, sql, args):
        self._cursor.execute(sql, args)
        result = self._cursor.fetchone()
        return result

    def fetch_all(self, sql, args):
        self._cursor.execute(sql, args)
        result = self._cursor.fetchall()
        return result

    def __del__(self):
        self._connection.close()
db_connection = SqlPooled()


select_query ="""
SELECT * from tablename where resource_id = {} and resource_type = {} limit 1
"""

insert_query = """
INSERT INTO tablename (data, type) VALUES 
"""

update_query = """
UPDATE tablename SET data = %s, type = %s;
"""

delete_query = """
DELETE FROM  tablename where resource_id = {} and resource_type = {}
"""

select_all_query = """
SELECT * from tablename
"""

def delete_from_d4cfhirdatastore(db_connection,resource_id, resource_type):
    try:
        if resource_id and resource_type:
            delete__query_format = delete_query.format(
                    str("'")+resource_id+str("'"),
                    str("'")+resource_type+str("'")
            )
            db_connection._cursor.execute(delete__query_format)
            return json.dumps({"error":"success"})
    except:
        return json.dumps({"error":"Deletion error"})

def get_from_d4cfhirdatastore(db_connection,resource_id,resource_type):
    try:
        if resource_id and resource_type:
            select_query_format = select_query.format(
                    str("'")+resource_id+str("'"),
                    str("'")+resource_type+str("'"),
            )
            db_connection._cursor.execute(select_query_format)
            df = db_connection._cursor.fetchall()
            logging.warning(df)
            return json.loads(df)
        else:
            db_connection._cursor.execute(select_all_query)
            df = db_connection._cursor.fetchall()
            list = []
            for i in range(len(df)):
                list.append(df[i])
            return json.loads(list)
    except:
        return json.dumps({"error":"Fetch error"})

def is_exists(db_connection, resource_id, resource_type):
    db_connection._cursor.execute(select_query.format(str("'") + str(resource_id) + str("'"), str("'") + resource_type + str("'")))
    for row in db_connection._cursor.fetchall():
        logging.info("id exists")
        return row
    return None

def save_to_d4cfhirdatastore(resource_id,resource_type,resource_json,db_connection):

    logging.info(type(resource_json))
    resource_json_after_dump = json.dumps(resource_json, indent=4, sort_keys=True)
    logging.info(type(resource_json_after_dump))
    columns = {}

    columns["data"]=resource_json_after_dump
    columns["type"]=resource_type
    
    logging.info("values set to column")
    logging.info("values set to column")
    print("values into the db")
    logging.info("checking id value")
    
    id_value = is_exists(db_connection, resource_id, resource_type)
    logging.info(id_value)
    print(id_value)
    if id_value:
        print("intto the if id")
        logging.info("Update path")
        columns["id"] = id_value["resource_id"]
        value = (
                    columns["data"],
                    columns["type"]
                )
        db_connection._cursor.execute(update_query, value)
    else:
        logging.info("Update path")
        print("into the else")
        value = (
                    columns["data"],
                    columns["type"]
                )
        db_connection._cursor.execute(select_query, value)
    print("data inserted ")
    logging.info("data inserted successfully !! ")
    return "success"


