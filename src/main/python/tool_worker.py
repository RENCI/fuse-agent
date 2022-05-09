from rq import Worker, Queue, Connection

from main import g_redis_connection, g_queue

if __name__ == '__main__':
    with Connection(g_redis_connection):
        worker = Worker(g_queue, connection=g_redis_connection)
        worker.work()
