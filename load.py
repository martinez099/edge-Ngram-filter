import random
import redis
import string
import multiprocessing


#host = 'localhost'
host = 'redis-18125.internal.martin.demo.redislabs.com'
#port = 6379
port = 18125

batch_size = 10000
cores = 10
entries_per_core = 50000

r = redis.StrictRedis(host=host, port=port)


def random_string(length=10):
    """Generate a random string of fixed length """
    letters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))


def populate(amount_per_core):
    while amount_per_core > 0:
        with r.pipeline(transaction=False) as pipe:
            for _ in range(0, min(batch_size, amount_per_core)):
                _id = random_string()
                key = 'prefix:{}'.format(_id)
                pipe.hset('part:{}'.format(_id[0]), key, random_string(2000))
            pipe.execute()
        amount_per_core -= batch_size


def lookup(prefix):
    cursor = -1
    found = []
    while cursor != 0:
        cursor = 0
        cursor, result = r.hscan('part:{}'.format(prefix[0]), cursor, match='prefix:{}*'.format(prefix), count=1000)
        found.append(result)

    return found


r.flushdb()

procs = []
for i in range(0, cores):
    p = multiprocessing.Process(target=populate, args=(entries_per_core,))
    procs.append(p)
    p.start()

print('waiting ...')
[p.join() for p in procs]