import multiprocessing
import random
import redis
import string
import threading
import time

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

LOAD_BATCH_SIZE = 10000
LOAD_ENTRIES_PER_CLIENT = 50000

SEARCH_CLIENTS = 10
SEARCH_THREADS_PER_CLIENT = 10

LOOKUP_COUNT = 10
LOOKUP_NGRAM_SIZE = 2


def random_string(_length=10):
    """Generate a random string of fixed length"""
    letters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(letters) for _ in range(_length))


def populate(_amount_per_core, _redis):
    while _amount_per_core > 0:
        with _redis.pipeline(transaction=False) as pipe:
            for _ in range(0, min(LOAD_BATCH_SIZE, _amount_per_core)):
                _id = random_string()
                key = 'prefix:{}'.format(_id)
                pipe.hset('part:{}'.format(_id[0]), key, random_string(2000))
            pipe.execute()
        _amount_per_core -= LOAD_BATCH_SIZE


def load(_redis, _cores=10):
    procs = []
    for i in range(0, _cores):
        p = multiprocessing.Process(target=populate, args=(LOAD_ENTRIES_PER_CLIENT, _redis))
        procs.append(p)
        p.start()

    print('loading ...')
    [p.join() for p in procs]


def lookup(_prefix, _redis):
    found = []
    cursor, result = _redis.hscan('part:{}'.format(_prefix[0]), 0, match='prefix:{}*'.format(_prefix), count=LOOKUP_COUNT)
    while cursor != 0:
        cursor, result = _redis.hscan('part:{}'.format(_prefix[0]), cursor, match='prefix:{}*'.format(_prefix), count=LOOKUP_COUNT)
        found.extend(result)

    print('found {} entires'.format(len(found)))


def lookups(_redis):
    ts = []
    for i in range(0, SEARCH_THREADS_PER_CLIENT):
        t = threading.Thread(target=lookup, args=(random.choice(string.ascii_uppercase + string.digits) * LOOKUP_NGRAM_SIZE, _redis))
        ts.append(t)
        t.start()
    [t.join() for t in ts]


def search(_redis):
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


r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
r.flushdb()

load(r)
search(r)

print('done.')
