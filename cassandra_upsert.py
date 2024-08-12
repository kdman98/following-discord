import random

from cassandra.cluster import Cluster
import time
import uuid

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS test
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")
session.execute("""
CREATE TABLE IF NOT EXISTS test.test_table (
    id UUID PRIMARY KEY,
    channel_id INT,
    data TEXT
)
""")

row_id = uuid.uuid4()


# Insert initial row
def insert_row():
    session.execute(
        "INSERT INTO test.test_table (id, channel_id, data) VALUES (%s, %s, %s)",
        (row_id, random.randint(1, 100000), 'initial_value')
    )


def edit_row():
    session.execute("UPDATE test.test_table SET data = %s WHERE id = %s", ('updated_value', row_id))
    # session.execute("UPDATE test.test_table SET data = %s WHERE id = %s IF EXISTS", ('updated_value', row_id))
    time.sleep(0.01)


def delete_row():
    session.execute("DELETE FROM test.test_table WHERE id = %s", (row_id,))
    time.sleep(0.01)


insert_row()
delete_row()
edit_row()

final_state = session.execute("SELECT * FROM test.test_table WHERE id = %s", (row_id,)).one()
print(final_state)

row_id = uuid.uuid4()
insert_row()
edit_row()

final_state = session.execute("SELECT * FROM test.test_table WHERE id = %s", (row_id,)).one()
print(final_state)

session.execute("DROP TABLE test.test_table")
session.execute("DROP KEYSPACE test")
cluster.shutdown()
