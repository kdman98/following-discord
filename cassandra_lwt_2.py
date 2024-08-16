from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
import uuid
import random

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Setup keyspace and table
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS test_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS test_keyspace.messages (
        id UUID PRIMARY KEY,
        channel_id UUID,
        content TEXT,
        timestamp TIMESTAMP
    )
""")


# Function to simulate message insertion
def insert_message(id):
    channel_id = uuid.uuid4()
    query = SimpleStatement(
        "INSERT INTO test_keyspace.messages (id, channel_id, content, timestamp) VALUES (%s, %s, %s, toTimestamp(now()))",
        consistency_level=1
    )
    session.execute(query, (id, channel_id, "initial message"))


def update_message(id, content):
    session.execute(
        "UPDATE test_keyspace.messages SET content = %s, timestamp = toTimestamp(now()) WHERE id = %s",
        (content, id)
    )


def update_message_lwt(id, content):
    session.execute(
        "UPDATE test_keyspace.messages SET content = %s, timestamp = toTimestamp(now()) WHERE id = %s IF EXISTS",
        (content, id)
    )


def discord_approach(id):
    content = None if random.random() < 0.01 else "discord approach content"
    update_message(id, content)
    session.execute(
        "SELECT * FROM test_keyspace.messages WHERE id = %s",
        (id,)
    ).one()

    # Check if the message is corrupt (content is None) and delete it
    if content is None:
        session.execute("DELETE FROM test_keyspace.messages WHERE id = %s", (id,))


def lwt_approach(id):
    content = "lwt approach content"
    update_message_lwt(id, content)


# Performance test
def performance_test():
    discord_times = []
    lwt_times = []
    id_list = []

    performance_test_initial_rows = 1000000
    for _ in range(performance_test_initial_rows):
        id = uuid.uuid4()
        insert_message(id)
        id_list.append(id)

    experiment_repetitions = 1000
    for i in range(experiment_repetitions):
        start = time.time()
        discord_approach(id_list[i])
        discord_times.append(time.time() - start)

    for i in range(experiment_repetitions):
        start = time.time()
        lwt_approach(id_list[i])
        lwt_times.append(time.time() - start)

    print(f"Average time for LWT approach: {sum(lwt_times) / len(lwt_times) * 1000} ms")
    print(f"Average time for Discord approach: {sum(discord_times) / len(discord_times) * 1000} ms")


# Run the performance test
performance_test()

# Cleanup
session.execute("DROP KEYSPACE test_keyspace")
cluster.shutdown()
