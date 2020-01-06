import multiprocessing
import random
import redis
import string
import threading
import time

C = 10  # paging size
N = 2  # length of characters in a gram


def random_string(_length=10):
    """Generate a random string of fixed length"""
    letters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(letters) for _ in range(_length))


def populate(_amount_per_core, _redis, _batch_size=10000):
    while _amount_per_core > 0:
        with _redis.pipeline(transaction=False) as pipe:
            for _ in range(0, min(_batch_size, _amount_per_core)):
                _id = random_string()
                key = 'prefix:{}'.format(_id)
                pipe.hset('part:{}'.format(_id[0]), key, random_string(2000))
            pipe.execute()
        _amount_per_core -= _batch_size


def load(_redis, _cores=10, _entries_per_core=50000):
    procs = []
    for i in range(0, _cores):
        p = multiprocessing.Process(target=populate, args=(_entries_per_core, _redis))
        procs.append(p)
        p.start()

    print('loading ...')
    [p.join() for p in procs]


def lookup(_prefix, _redis):
    found = []
    cursor, result = _redis.hscan('part:{}'.format(_prefix[0]), 0, match='prefix:{}*'.format(_prefix), count=C)
    while cursor != 0:
        cursor, result = _redis.hscan('part:{}'.format(_prefix[0]), cursor, match='prefix:{}*'.format(_prefix), count=C)
        found.extend(result)

    print('found {} entires'.format(len(found)))


def lookups(_redis, _threads=10):
    ts = []
    for i in range(0, _threads):
        t = threading.Thread(target=lookup, args=(random.choice(string.ascii_uppercase + string.digits) * N, _redis))
        ts.append(t)
        t.start()
    [t.join() for t in ts]


def search(_redis, _cores=10):
    procs = []
    for i in range(0, _cores):
        p = multiprocessing.Process(target=lookups, args=(_redis,))
        procs.append(p)
        p.start()

    print('searching ...')
    then = time.time()
    [p.join() for p in procs]
    now = time.time()
    print(now - then)


host = 'localhost'
port = 6379

r = redis.StrictRedis(host=host, port=port)
r.flushdb()

load(r)
search(r)

print('done.')
