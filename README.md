# feed-fanout

How Facebook's News Feed delivers posts, built step by step: from an in-memory queue to a distributed, fault-tolerant pipeline.

System design interviews talk about queues, workers, and "fanout-on-write" through boxes and arrows. This repo turns that diagram into code you can run. The project grows in tagged versions, and each version fixes what broke in the one before it.

## The design

When a user posts, we don't build everyone's feed on the spot. We save the post, drop a message on a queue, and return immediately. Background workers pull from the queue and copy the post ID into each follower's precomputed feed. Reading a feed then becomes a cheap lookup.

```
create_post() ──► posts DB
      │
      └──► queue ──► [worker-1, worker-2, worker-3] ──► feeds (one list per user)
                                                            │
                                            get_feed() ◄────┘
```

**The trade-off:** posting is fast and reads are cheap, but feeds are *eventually consistent*. A new post takes a moment to show up, and authors with huge follower counts (the "celebrity problem") take much longer.

## Evolution

`main` always holds the latest version. Each version is a git tag, so you can browse its code or diff it against the previous one.

| Version | What it adds | What it teaches | Code |
|---|---|---|---|
| **v1-in-memory** | `queue.Queue`, worker threads, dicts as databases | Decoupling writes from slow work, producer/consumer, eventual consistency | [browse](https://github.com/gizawNahom/feed-fanout/tree/v1-in-memory) |
| v2-redis *(next)* | Redis as queue and feed store, API and workers as separate processes | Durability, scaling workers independently | |
| v3-reliability *(planned)* | Acks and retries, celebrity handling, chunked fanout | At-least-once delivery, hybrid fanout | |

To run a specific version:

```bash
git checkout v1-in-memory
```

## Quick start

Requires Python 3.9+. There are no dependencies.

```bash
git clone https://github.com/gizawNahom/feed-fanout.git
cd feed-fanout
python3 feed.py
```

What to look for in the output:

1. Every `api ... enqueued` line has a timestamp around `0.001s`. **Posting is instant** because no fanout happens during the request.
2. `bob's feed right now: []`: the post exists, but fanout hasn't reached bob yet. That's **eventual consistency**.
3. `worker-1/2/3 picked ...`: several workers drain the queue in parallel, and each message goes to exactly one worker.

## How the code maps to the design

| Design piece | In `feed.py` |
|---|---|
| Posts DB | `posts` dict |
| Follow graph | `followers` dict: user → who follows them |
| Precomputed feeds (Redis in production) | `feeds` dict: user → list of post IDs |
| Queue | `queue.Queue` from the standard library |
| Fanout workers | threads running `worker()` |
| Post Service | `create_post()` |
| Feed Service | `get_feed()` |

## Play with it interactively

```python
import threading
from feed import *

follow("bob", "alice")
workers = [threading.Thread(target=worker, args=(f"worker-{i+1}",)) for i in range(2)]
for t in workers: t.start()

create_post("alice", "hello")
get_feed("bob")

# The celebrity problem: 200 followers ties up one worker for ~10 seconds
for i in range(200): follow(f"fan{i}", "taylor")
follow("bob", "taylor")
create_post("taylor", "new album!"); get_feed("bob")[:1]   # not there yet
get_feed("bob")[:1]                                        # run again a few seconds later

for _ in workers: fanout_queue.put(STOP)   # shut down cleanly before exit()
```

## Key ideas

- **`queue.Queue` is thread-safe.** `get()` blocks until work arrives, so idle workers use no CPU. `task_done()` and `join()` track when work is *finished*, not just picked up.
- **Keep locks short.** `feeds_lock` guards only the list insert. The slow write stays outside it, or the workers would end up running one at a time.
- **Workers finish out of order.** Two posts from the same author can land in a feed in the wrong order, so `get_feed()` sorts by timestamp instead of trusting insert order.
- **Graceful shutdown.** Wait for the queue to drain, send one `STOP` per worker, and join each thread.
- **Threads suit this work despite the GIL** because fanout is I/O-bound (simulated here with `sleep`).

## Current limitations

v1 shows the *pattern*, but it isn't a real system yet:

- **No durability.** If the process crashes, every queued post and every feed is lost.
- **No distribution.** The API and workers share one process, so they can't scale independently on separate machines.
- **No retries.** A worker that dies mid-fanout leaves some followers without the post.
- **Celebrities block workers.** One author with millions of followers ties up a worker for a very long time.

v2 fixes the first two by moving the queue and feeds into Redis and running workers as separate processes.

## License

[MIT](LICENSE)
