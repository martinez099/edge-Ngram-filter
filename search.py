import random
import redis
import string
import time
import multiprocessing
import threading


#host = 'localhost'
host = 'redis-18125.internal.martin.demo.redislabs.com'
#port = 6379
port = 18125

count = 10
cores = 10
threads = 10

r = redis.StrictRedis(host=host, port=port)


def lookup(prefix):
    found = []
    cursor, result = r.hscan('part:{}'.format(prefix[0]), 0, match='prefix:{}*'.format(prefix), count=count)
    while cursor != 0:
        cursor, result = r.hscan('part:{}'.format(prefix[0]), cursor, match='prefix:{}*'.format(prefix), count=count)
        found.append(result)

    print('found {} entires'.format(len(found)))


def lookups():
    ts = []
    for i in range(0, threads):
        t = threading.Thread(target=lookup, args=(random.choice(string.ascii_uppercase + string.digits) * 2,))
        ts.append(t)
        t.start()
    [t.join() for t in ts]


procs = []
for i in range(0, cores):
    p = multiprocessing.Process(target=lookups)
    procs.append(p)
    p.start()

print('waiting ...')

now = time.time()

[p.join() for p in procs]

then = time.time()

print(then - now)