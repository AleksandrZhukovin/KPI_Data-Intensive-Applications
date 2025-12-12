import threading
import time
from pymongo import MongoClient, WriteConcern
from pymongo.errors import AutoReconnect


def reset():
    client = MongoClient("mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0")
    db = client["lab_db"]
    db["counter"].delete_many({})
    db["counter"].insert_one({"name": "post_1", "likes": 0})
    client.close()


def worker(w_concern):
    if w_concern == 'majority':
        wc = WriteConcern(w='majority', wtimeout=10000)
    else:
        wc = WriteConcern(w=1)

    client = MongoClient("mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0")
    db = client.get_database("lab_db", write_concern=wc)
    collection = db["counter"]

    for _ in range(10000):
        try:
            collection.find_one_and_update(
                {"name": "post_1"},
                {"$inc": {"likes": 1}}
            )
        except AutoReconnect:
            time.sleep(0.1)
        except Exception:
            pass

    client.close()


def run(test_name, w_concern):
    print(f"\n{test_name}")
    reset()

    threads = []
    start_time = time.time()

    for i in range(10):
        t = threading.Thread(target=worker, args=(w_concern,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    duration = end_time - start_time

    client = MongoClient("mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0")
    final_doc = client.get_database("lab_db", write_concern=WriteConcern(w='majority'))["counter"].find_one(
        {"name": "post_1"})

    client.close()

    final_likes = final_doc['likes'] if final_doc else 0
    print(f"Time: {duration:.2f}s")
    print(f"likes: {final_likes}\nexpected: 100000")


run("Test #1", 1)
run("Test #2", 'majority')

input("Enter when PRIMARY is down")
run("Test #3", 1)

input("Enter when PRIMARY is down")
run("Test #4", 'majority')