'''Simple Extract and Load Python Program'''

import petl, psycopg2

from sqlalchemy import *

# declare connection properties within dictionary
dbCnxns = {"python_test" : "dbname = test_python user = postgres host = 127.0.0.1 password = postgres",
"simple_etl":"dbname = simple_etl user = postgres host = 127.0.0.1 password = postgres"}

#set connections and cursors
source_connection = psycopg2.connect(dbCnxns["python_test"])
target_connection = psycopg2.connect(dbCnxns["simple_etl"])
source_cursor = source_connection.cursor()
target_cursor = target_connection.cursor()

# retrieve the names of the source table to be copied
source_cursor.execute('''SELECT table_name FROM information_schema.tables
WHERE table_name = 'transactions' ''')

source_table = source_cursor.fetchall()

# iterate through table to copy over
for record in source_table:
    target_cursor.execute("DROP TABLE IF EXISTS %s" % (record[0]))
    source_database = petl.fromdb(source_connection, "SELECT * FROM %s" % (record[0]))
    petl.todb(source_database,target_connection,record[0],create=True,sample=10)
