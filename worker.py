from queueModel import QueueModel
from workerModel import WorkerModel
from profiler import profiler
import json
import time
from jobs import *



class Worker(WorkerModel):
    def __init__(self, w_id):
        super(Worker, self).__init__(w_id)
        self.queue = QueueModel(w_id)
        self.state = {}


    def log(self, text):
        print(text)


    def initialize(self):
        print("init from worker")

    def start(self):
        self.loop()

    def loop(self):
        while True:
            self._sync()
            while self.state["state"] == "RUNNING":
                self._sync()
                if self.state["command"] == "PLAY":
                    self.work()
                if self.state["command"] == "PAUSE":
                    time.sleep(1)
                if self.state["command"] == "STOP":
                    time.sleep(1)
            if self.state["state"] == "INITIALIZING":
                print("init")
            if self.state["state"] == "INITIALIZING":
                print("init")
            
            time.sleep(1)

    def work(self):
        jobs = self.queue.get_jobs()
        print(jobs)

        if (len(jobs) == 0):

            time.sleep(5)
        else:   
            for job in jobs:
                print(job)
                self.queue.start_job(job['id'])
                res = self.do_job(job)
                self.queue.return_job(job['id'], res[0])
  

    @profiler
    def do_job(self, args):
        job_result = eval("{}({})".format(args['function'], args['args']))
        return job_result
        


