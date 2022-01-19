import pymysql


def openDb():
   global conn, cursor
   conn = pymysql.connect(
       host="localhost",
       user="root",
       password="",
       db="db_kafka" )
   cursor = conn.cursor()	

#db closing conn
def closeDb():
   global conn, cursor
   cursor.close()
   conn.close()

def test_db():
    openDb()
    sql = "SELECT kata from Msg"
    cursor.execute(sql)
    rs = cursor.fetchall()
    assert len(rs) == 9 

test_db()