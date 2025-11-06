import threading
import psycopg2
import time
from psycopg2.extensions import ISOLATION_LEVEL_DEFAULT, ISOLATION_LEVEL_SERIALIZABLE
from psycopg2.pool import ThreadedConnectionPool


DB_URL = "postgresql://postgres:12345@localhost:5432/postgres"

pool = ThreadedConnectionPool(minconn=10, maxconn=15, dsn=DB_URL)


def db():
    conn = psycopg2.connect(DB_URL)
    conn.set_isolation_level(ISOLATION_LEVEL_DEFAULT)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS user_counter")
        cur.execute("""
            CREATE TABLE user_counter (
                user_id INT PRIMARY KEY,
                counter INT NOT NULL DEFAULT 0,
                version INT NOT NULL DEFAULT 0
            )
        """)
        cur.execute(
            "INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)"
        )
    conn.commit()
    conn.close()


def get_counter():
    conn = psycopg2.connect(DB_URL)
    with conn.cursor() as cur:
        cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        result = cur.fetchone()
    conn.close()
    return result[0] if result else 0


def worker(target_function):
    conn = pool.getconn()
    target_function(conn)
    pool.putconn(conn)


def lost_update(conn):
    for _ in range(10000):
        with conn.cursor() as cur:
            cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            counter = cur.fetchone()[0]
            counter += 1
            cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))
        conn.commit()


def serializable1(conn):
    conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
    for _ in range(10000):
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                counter = cur.fetchone()[0]
                counter += 1
                cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))
            conn.commit()
        except psycopg2.errors.SerializationFailure:
            conn.rollback()
    conn.set_isolation_level(ISOLATION_LEVEL_DEFAULT)


def serializable2(conn):
    conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
    for _ in range(10000):
        while True:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                    counter = cur.fetchone()[0]
                    counter += 1
                    cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))
                conn.commit()
                break
            except psycopg2.errors.SerializationFailure:
                conn.rollback()
    conn.set_isolation_level(ISOLATION_LEVEL_DEFAULT)


def in_place_update(conn):
    for _ in range(10000):
        with conn.cursor() as cur:
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
        conn.commit()


def row_level_lock(conn):
    for _ in range(10000):
        with conn.cursor() as cur:
            cur.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
            counter = cur.fetchone()[0]
            counter += 1
            cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter, ))
        conn.commit()


def optimistic_concurrency_control(conn):
    for _ in range(10000):
        while True:
            with conn.cursor() as cur:
                cur.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                result = cur.fetchone()
                counter, version = result[0], result[1]

                new_counter = counter + 1
                new_version = version + 1

                cur.execute(
                    "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = 1 AND version = %s",
                    (new_counter, new_version, version)
                )
                updated_rows = cur.rowcount

            conn.commit()

            if updated_rows > 0:
                break
            else:
                conn.rollback()


def run(name, target_function):
    db()
    threads = []
    print(name)
    start = time.time()
    for _ in range(10):
        t = threading.Thread(target=worker, args=(target_function,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    finish = time.time() - start
    c = get_counter()

    print(f"Must be: 100000")
    print(f"Counter: {c}")
    print(f"Time: {finish:.2f}s\n\n")


run("Lost update", lost_update)
run("Serializable 1", serializable1)
run("Serializable 2", serializable2)
run("In-place update", in_place_update)
run("Row-level lock", row_level_lock)
run("Optimistic concurrency control", optimistic_concurrency_control)

pool.closeall()
