import time
import threading
import hazelcast


def counter(client):
    m = client.get_map("counter").blocking()
    for _ in range(10000):
        v = m.get("counter_key")
        if v is None:
            v = 0
        m.put("counter_key", int(v) + 1)


def run():
    client = hazelcast.HazelcastClient(smart_routing=False)
    m = client.get_map("counter").blocking()
    m.clear()
    start = time.time()
    threads = [threading.Thread(target=counter, args=(client,)) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    finish = time.time() - start
    c = m.get("counter_key")
    print(f"Time: {finish:.3f}\ncounter: {c}\nmust be: 100000")
    client.shutdown()


run()
