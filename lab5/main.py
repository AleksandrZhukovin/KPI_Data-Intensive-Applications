from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import threading
import time


row_id = 1


def run_client(consistency_level):
    cluster = Cluster(['cs-1'], port=9042)
    session = cluster.connect('space3')

    query = f"UPDATE counter SET count = count + 1 WHERE id = {row_id}"
    statement = SimpleStatement(query, consistency_level=consistency_level)

    for _ in range(10000):
        session.execute(statement)
    cluster.shutdown()


def run(consistency_level, level_name):
    global row_id
    row_id += 1

    print(f"Test {level_name}")
    start_time = time.time()

    threads = []
    for _ in range(10):
        t = threading.Thread(target=run_client, args=(consistency_level,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()
    duration = end_time - start_time

    cluster = Cluster(['cs-1'], port=9042)
    session = cluster.connect('space3')

    row = session.execute(f"SELECT count FROM counter WHERE id = {row_id}").one()
    final_count = row.count if row else 0
    cluster.shutdown()

    print(f"Time: {duration:.2f}s")
    print(f"Count: {final_count}|Expected: 100000\n")


run(ConsistencyLevel.ONE, "CONSISTENCY ONE")
run(ConsistencyLevel.QUORUM, "CONSISTENCY QUORUM")
