import random
import time
from pymongo import MongoClient

# Connect to the MongoDB server running on localhost at the default port 27017
client = MongoClient('mongodb://localhost:27017/')

# Create (or switch to) a database named 'example_db'
db = client['test_db']
# Create (or switch to) a collection named 'example_collection'
db.drop_collection('test')
collection = db['test']

document_list = []
initial_document_counts = 20000000
additional_request_counts = 10
repeat_times = 100

# remove original documents
query_result = collection.delete_many({})
print("Original Removing Query result:", query_result)

# insert many documents, with desired indexes
collection.create_index("message_id")
collection.create_index("author_id")
collection.create_index("content")
collection.create_index("server_time")

for i in range(1, initial_document_counts + 1):
    document = {
        "_id": i,
        "message_id": i,
        "author_id": 1,
        "content": 1,
        "server_time": 1
    }
    document_list.append(document)
insert_result = collection.insert_many(document_list)  # TODO: batch too

print("Insert Done")

print("Elapsed time(ms):")
max = 0
min = 100000
sum_time = 0
for repeat in range(1, repeat_times + 1):
    # and then read / write at 50:50 ratio
    start_time = time.time()

    for i in range(1, additional_request_counts + 1):
        read_result = collection.find_one({"_id": random.randint(1, initial_document_counts)})
        document_insert = {
            "_id": i + initial_document_counts + repeat * additional_request_counts,
            "message_id": i + initial_document_counts,
            "author_id": 1,
            "content": 1,
            "server_time": random.randint(1, 1000000)
        }
        write_result = collection.insert_one(document_insert)


    end_time = time.time()
    elapsed_time = (end_time - start_time) * 1000
    if max < elapsed_time:
        max = elapsed_time
    if min > elapsed_time:
        min = elapsed_time
    sum_time += elapsed_time

print(max, min, sum_time / repeat_times)

# query_result = collection.delete_many({})
# print("Query result:", query_result)



# Close the connection
client.close()
