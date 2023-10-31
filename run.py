import os

from worker import Worker

if __name__ == '__main__':
    worker = Worker(1)
    worker.start()

