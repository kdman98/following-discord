import random
import time
import uuid

import cassandra
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Connect to the Cassandra cluster
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

# Insert initial row TODO: more insertion, full partition
session.execute(
    "INSERT INTO test.test_table (id, channel_id, data) VALUES (%s, %s, %s)",
    (row_id, random.randint(1, 100000), 'initial_value')
)

# Prepare the data
new_data = "updated_value"


def lwt_update():
    # Measure the time for LWT update
    start_time = time.time()
    lwt_update_query = "UPDATE test.test_table SET data = %s WHERE id = %s IF EXISTS"
    session.execute(lwt_update_query, (new_data, row_id))
    end_time = time.time()
    elapsed_time = (end_time - start_time) * 1000
    return elapsed_time


def all_update():
    # Measure time for ALL consistency level update
    start_time = time.time()
    search_result = session.execute("SELECT * FROM test.test_table WHERE id = %s", (row_id,)).one()
    if search_result is not None:
        all_update_query = SimpleStatement(
            query_string="UPDATE test.test_table SET data = %s WHERE id = %s",
            consistency_level=cassandra.ConsistencyLevel.ALL
        )
        session.execute(all_update_query, (new_data, row_id))
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000
    return elapsed_time


experiment_repeat_number = 100
lwt_time_sum = 0
all_update_time_sum = 0

for _ in range(experiment_repeat_number):
    lwt_time = lwt_update()
    all_update_time = all_update()
    lwt_time_sum += lwt_time
    all_update_time_sum += all_update_time

print(f"LWT Update Time AVG: {lwt_time_sum / experiment_repeat_number:.2f} ms")
print(f"ALL consistency level Update Time AVG: {all_update_time_sum / experiment_repeat_number:.2f} ms")

session.execute("DROP TABLE test.test_table")
session.execute("DROP KEYSPACE test")
cluster.shutdown()
