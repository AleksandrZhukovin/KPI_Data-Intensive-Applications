import argparse
import requests
import time
from multiprocessing import Process


parser = argparse.ArgumentParser()
parser.add_argument("--clients", "-c", help="Amount of clients to run", type=int, default=1)
args = parser.parse_args()


def client():
    for _ in range(10000):
        requests.get("http://web:8000/increase/")


if __name__ == "__main__":
    total_requests = 10000 * args.clients

    clients = []
    time.sleep(10)

    start = time.time()
    for i in range(args.clients):
        process = Process(target=client)
        clients.append(process)
        process.start()

    for process in clients:
        process.join()
    end = time.time()

    duration = end - start
    f = open('file.txt', 'w')
    f.write(f"Sent {total_requests} requests in {duration:.2f}s\nRequests per second: {total_requests / duration:.5f}")
