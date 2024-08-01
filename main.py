import random
import time
from pymongo import MongoClient

# Connect to the MongoDB server running on localhost at the default port 27017
client = MongoClient('mongodb://localhost:27017/')

# Create (or switch to) a database named 'example_db'
db = client['test_db']
# Create (or switch to) a collection named 'example_collection'
collection = db['test']

start_time = time.time()

for i in range(10):
    document = {
      "channel_id": i,
      "message_id": i,
      "author_id": 1,
      "content": 1
    }
    insert_result = collection.insert_one(document) # TODO: batch too
    print(f"Inserted document with _id: {insert_result.inserted_id}")

# Query the collection for the inserted document
query_result = collection.delete_many({})
print("Query result:", query_result)

end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time:", elapsed_time * 1000, "ms")

# Close the connection
client.close()
