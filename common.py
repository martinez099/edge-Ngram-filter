import random
import string

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

KEY_LENGTH = 10
VALUE_LENGTH = 2000

LOAD_CLIENTS = 10
LOAD_ENTRIES_PER_CLIENT = 50000
LOAD_BATCH_SIZE = 10000

SEARCH_CLIENTS = 10
SEARCH_THREADS_PER_CLIENT = 10

LOOKUP_COUNT = 10
LOOKUP_NGRAM_SIZE = 2


def random_string(_length):
    """Generate a random string of fixed length"""
    letters = string.ascii_uppercase + string.digits
    return ''.join(random.choice(letters) for _ in range(_length))
