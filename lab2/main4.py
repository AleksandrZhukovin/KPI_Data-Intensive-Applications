import time
import threading
import hazelcast


def counter(client):
    atom = client.cp_subsystem.get_atomic_long("counter").blocking()
    for _ in range(10000):
        atom.increment_and_get()


def run():
    client = hazelcast.HazelcastClient(
        cluster_name="cluster1",
        cluster_members=[
            "localhost:5701",
            "localhost:5702",
            "localhost:5703"
        ]
    )
    atom = client.cp_subsystem.get_atomic_long("counter").blocking()
    atom.set(0)
    start = time.time()
    threads = [threading.Thread(target=counter, args=(client,)) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    finish = time.time() - start
    c = atom.get()
    print(f"Time: {finish:.3f}\ncounter: {c}\nmust be: 100000")
    client.shutdown()


run()
