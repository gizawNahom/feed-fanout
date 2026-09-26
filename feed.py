r"""
News feed fanout-on-write, all in one process.

    create_post() --> posts DB
                  \-> queue --> [worker-1, worker-2, worker-3] --> feeds (per-follower lists)

The "DBs" are dicts, the queue is queue.Queue, workers are threads.
"""

import queue
import random
import threading
import time
import uuid
from collections import defaultdict

# ---------------------------------------------------------------------------
# "Databases"
# ---------------------------------------------------------------------------
posts = {}                     # post_id -> {id, author, text, ts}
followers = defaultdict(set)   # user -> set of users who follow them
feeds = defaultdict(list)      # user -> list of post_ids (newest first)
feeds_lock = threading.Lock()  # workers write to feeds concurrently

# ---------------------------------------------------------------------------
# The queue
# ---------------------------------------------------------------------------
fanout_queue = queue.Queue()
STOP = None  # sentinel telling a worker to exit

start_time = time.time()


def log(who, msg):
    print(f"[{time.time() - start_time:6.3f}s] {who:<10} {msg}")


# ---------------------------------------------------------------------------
# Post Service (write path)
# ---------------------------------------------------------------------------
def follow(follower, followee):
    followers[followee].add(follower)


def create_post(author, text):
    post_id = uuid.uuid4().hex[:6]
    posts[post_id] = {"id": post_id, "author": author, "text": text, "ts": time.time()}

    # Don't fan out here -- just enqueue and return. This is the whole point.
    fanout_queue.put({"post_id": post_id, "author": author})
    log("api", f"{author} posted {post_id!r} -> enqueued (queue size={fanout_queue.qsize()})")
    return post_id


# ---------------------------------------------------------------------------
# Fanout workers
# ---------------------------------------------------------------------------
def worker(name):
    while True:
        msg = fanout_queue.get()  # blocks until a message arrives
        if msg is STOP:
            fanout_queue.task_done()
            log(name, "stopping")
            return

        post_id, author = msg["post_id"], msg["author"]
        targets = followers[author]
        log(name, f"picked {post_id!r} from {author}, fanning out to {len(targets)} followers")

        for f in targets:
            time.sleep(random.uniform(0.02, 0.08))  # simulate a slow write (e.g. to Redis)
            with feeds_lock:
                feeds[f].insert(0, post_id)

        log(name, f"done with {post_id!r}")
        fanout_queue.task_done()


# ---------------------------------------------------------------------------
# Feed Service (read path) -- just reads the precomputed list
# ---------------------------------------------------------------------------
def get_feed(user, limit=10):
    with feeds_lock:
        ids = list(feeds[user])
    # Workers can finish out of order, so sort by post time instead of trusting insert order.
    return sorted((posts[i] for i in ids), key=lambda p: p["ts"], reverse=True)[:limit]


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
def main():
    # Social graph
    for u in ["bob", "carol", "dave", "erin"]:
        follow(u, "alice")
    for u in ["alice", "carol"]:
        follow(u, "bob")
    follow("alice", "carol")

    # Start workers
    NUM_WORKERS = 3
    threads = [threading.Thread(target=worker, args=(f"worker-{i+1}",)) for i in range(NUM_WORKERS)]
    for t in threads:
        t.start()

    # Write path: notice every create_post returns immediately
    create_post("alice", "hello world")
    create_post("bob", "first post!")
    create_post("alice", "lunch was great")
    create_post("carol", "anyone up for chess?")
    create_post("bob", "queues are neat")

    log("api", f"all posts returned. bob's feed right now: {[p['id'] for p in get_feed('bob')]}  (fanout still in progress)")

    # Wait for workers to drain the queue
    fanout_queue.join()
    for _ in threads:
        fanout_queue.put(STOP)
    for t in threads:
        t.join()

    # Read path
    print("\n=== Feeds after fanout ===")
    for user in ["alice", "bob", "carol", "dave", "erin"]:
        print(f"\n{user}:")
        for p in get_feed(user):
            print(f"  [{p['id']}] {p['author']}: {p['text']}")


if __name__ == "__main__":
    main()
