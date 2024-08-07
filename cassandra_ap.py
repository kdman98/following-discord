from cassandra.cluster import Cluster
import threading
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
    value TEXT
)
""")

none_count = 0
some_count = 0

for i in range(100):
    print(i)

    # Insert initial row
    row_id = uuid.uuid4()
    session.execute("INSERT INTO test.test_table (id, value) VALUES (%s, %s)", (row_id, 'initial_value'))

    def edit_row():
        for _ in range(10):
            session.execute("UPDATE test.test_table SET value = %s WHERE id = %s", ('updated_value', row_id))
            time.sleep(0.1)

    def delete_row():
        for _ in range(10):
            session.execute("DELETE FROM test.test_table WHERE id = %s", (row_id,))
            time.sleep(0.1)

    edit_thread = threading.Thread(target=edit_row)
    delete_thread = threading.Thread(target=delete_row)

    edit_thread.start()
    delete_thread.start()

    time.sleep(0.1)

    edit_thread.join(0)
    delete_thread.join(0)

    final_state = session.execute("SELECT * FROM test.test_table WHERE id = %s", (row_id,)).one()
    if final_state is None:
        none_count += 1
    else:
        some_count += 1

print("None count:", none_count)
print("Row count:", some_count)

session.execute("DROP TABLE test.test_table")
session.execute("DROP KEYSPACE test")
cluster.shutdown()
