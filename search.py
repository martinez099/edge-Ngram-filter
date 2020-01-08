import multiprocessing
import redis
import threading
import time

from common import random_string, LOOKUP_COUNT, SEARCH_THREADS_PER_CLIENT, LOOKUP_NGRAM_SIZE, SEARCH_CLIENTS, \
    REDIS_HOST, REDIS_PORT


def lookup(_prefix, _redis):
    """Lookup keys with given prefix"""
    found = []
    cursor, result = _redis.hscan('part:{}'.format(_prefix[0]), 0, match='prefix:{}*'.format(_prefix), count=LOOKUP_COUNT)
    while cursor != 0:
        cursor, result = _redis.hscan('part:{}'.format(_prefix[0]), cursor, match='prefix:{}*'.format(_prefix), count=LOOKUP_COUNT)
        found.extend(result)

    print('found {} entires'.format(len(found)))


def lookups(_redis):
    """Lookip keys from multiple threads"""
    ts = []
    for i in range(0, SEARCH_THREADS_PER_CLIENT):
        t = threading.Thread(target=lookup, args=(random_string(LOOKUP_NGRAM_SIZE), _redis))
        ts.append(t)
        t.start()
    [t.join() for t in ts]


def search(_redis):
    """Search random keys from multiple clients"""
    procs = []
    for i in range(0, SEARCH_CLIENTS):
        p = multiprocessing.Process(target=lookups, args=(_redis,))
        procs.append(p)
        p.start()

    print('searching ...')
    then = time.time()
    [p.join() for p in procs]
    now = time.time()
    print(now - then)


# init Redis
r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)

# search keys
search(r)

print('done.')
