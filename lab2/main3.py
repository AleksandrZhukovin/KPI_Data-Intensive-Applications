import time
import threading
import hazelcast


def counter(client):
    m = client.get_map("counter").blocking()
    for _ in range(10000):
        while True:
            old = m.get("counter_key")
            if old is None:
                prev = m.put_if_absent("counter_key", 1)
                if prev is None:
                    break
                else:
                    continue
            else:
                new = int(old) + 1
                ok = m.replace_if_same("counter_key", old, new)
                if ok:
                    break
                continue


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
