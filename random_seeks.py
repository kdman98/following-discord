import random
import time
from pymongo import MongoClient

# Connect to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')
db = client['test_db']
collection = db['test_collection']

# Configuration
num_messages = 10000000  # Total number of messages
access_count = 10000  # Simulate access requests


# Function to initialize the database with sparse messages
def initialize_db(num_messages):
    collection.drop()  # Clear the collection if it already exists
    bulk_operations = []
    current_time = time.time()
    interval = 86400 * 7  # not important

    for i in range(num_messages):
        timestamp = current_time - i * interval
        message = {
            "_id": i + 1,
            "timestamp": timestamp,
            "message": f"Message {i + 1}"
        }
        bulk_operations.append(message)

    collection.insert_many(bulk_operations)
    print(f"Inserted {num_messages} messages.")


# simulate random access
def simulate_access_and_measure_delays(access_count, num_messages):
    delays = []

    access_ids = random.sample(range(1, num_messages + 1), access_count)
    for access_id in access_ids:
        start_time = time.time()
        result = collection.find_one({"_id": access_id})
        end_time = time.time()
        delay = (end_time - start_time) * 1000  # Convert to milliseconds
        delays.append(delay)

    return delays


# simulate accessing index nearby
def simulate_near_index_search(access_count, num_messages):
    delays = []
    initial_index = 1
    access_ids = range(initial_index, initial_index + access_count)
    for access_id in access_ids:
        start_time = time.time()
        result = collection.find_one({"_id": access_id})
        end_time = time.time()
        delay = (end_time - start_time) * 1000  # Convert to milliseconds
        delays.append(delay)

    return delays


# Initialize the database with sparse messages
initialize_db(num_messages)

collection.find_one({}) # initial search has delay

# Run the simulation and measure delays
# delays = simulate_access_and_measure_delays(access_count, num_messages)
delays = simulate_near_index_search(access_count, num_messages)

# Analyze the delays
average_delay = sum(delays) / len(delays)
max_delay = max(delays)
min_delay = min(delays)

print(f"Average delay: {average_delay:.5f} ms")
print(f"Max delay: {max_delay:.5f} ms")
print(f"Min delay: {min_delay:.5f} ms")

# Clean up
client.close()
