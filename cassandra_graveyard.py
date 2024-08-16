import cassandra
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement
import time
import uuid

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("DROP KEYSPACE test_keyspace")

# Setup keyspace and table
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS test_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS test_keyspace.messages (
        channel_id UUID,
        message_id UUID,
        content TEXT,
        content_second TEXT,
        PRIMARY KEY (channel_id, message_id)
    )
""")

messages_id_list = []


# Step 1: Insert a large number of messages
def insert_messages(channel_id, num_messages, batch_size=10000):
    batch = BatchStatement()

    for i in range(num_messages):
        if i % 100000 == 0:
            print(f'Inserting message {i}...')

        message_id = uuid.uuid4()
        messages_id_list.append(message_id)

        # Add the insert statement to the batch
        batch.add(
            session.prepare(
                "INSERT INTO test_keyspace.messages (channel_id, message_id, content, content_second) VALUES (?, ?, ?, ?)"
            ),
            (channel_id, message_id, "This is a message " + str(i), "222")
        )

        # Execute the batch when the batch size is reached
        if i % batch_size == 0 and i != 0:
            session.execute(batch)
            batch.clear()  # Clear the batch to start a new one

    # Execute any remaining statements in the batch
    if len(batch) > 0:
        session.execute(batch)

    return messages_id_list


# Step 2: Delete most of the messages
def delete_messages(channel_id):
    for id in messages_id_list:
        session.execute(
            "DELETE content, content_second FROM test_keyspace.messages WHERE channel_id = %s AND message_id = %s",
            (channel_id, id,)
        )


# Step 3: Query the channel to simulate a load
def load_channel(channel_id):
    start = time.time()
    rows = session.execute(
        "SELECT * FROM test_keyspace.messages WHERE channel_id = %s",
        (channel_id,),
    )
    messages = list(rows)
    end = time.time()
    print(f"Loaded {len(messages)} messages in {end - start} seconds")


# Step 4: Run the simulation
channel_id = uuid.uuid4()
print(channel_id)
num_messages = 100000

print("Inserting messages...")
insert_messages(channel_id, num_messages)

print("Deleting messages...")
delete_messages(channel_id)

insert_messages(channel_id, 1)

print("Loading channel...")
load_channel(channel_id)

# Cleanup
# session.execute("DROP KEYSPACE test_keyspace")
cluster.shutdown()
