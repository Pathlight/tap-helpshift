import queue


def consume_q(q):
    while True:
        try:
            yield q.get_nowait()
        except queue.Empty:
            return
