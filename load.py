import multiprocessing
import redis

from common import random_string, LOAD_BATCH_SIZE, KEY_LENGTH, VALUE_LENGTH, LOAD_CLIENTS, LOAD_ENTRIES_PER_CLIENT, \
    REDIS_HOST, REDIS_PORT


def populate(_amount_per_core, _redis):
    """Populate Redis with random values"""
    while _amount_per_core > 0:
        with _redis.pipeline(transaction=False) as pipe:
            for _ in range(0, min(LOAD_BATCH_SIZE, _amount_per_core)):
                _id = random_string(KEY_LENGTH)
                key = 'prefix:{}'.format(_id)
                pipe.hset('part:{}'.format(_id[0]), key, random_string(VALUE_LENGTH))
            pipe.execute()
        _amount_per_core -= LOAD_BATCH_SIZE


def load(_redis):
    """Load data from multiple clients into Redis"""
    procs = []
    for i in range(0, LOAD_CLIENTS):
        p = multiprocessing.Process(target=populate, args=(LOAD_ENTRIES_PER_CLIENT, _redis))
        procs.append(p)
        p.start()

    print('loading ...')
    [p.join() for p in procs]


# init Redis
r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
r.flushdb()

# load data
load(r)

print('done.')
