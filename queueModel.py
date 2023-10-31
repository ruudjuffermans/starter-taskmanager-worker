from db import Db
import json
import time

class QueueModel(Db):
    def __init__(self, w_id):
        super(QueueModel, self).__init__()
        self.w_id = w_id

        self.test123 = "55"

    def create_job(self, _def, _args):
        query="INSERT INTO `queue`(`def`, `args`) VALUES ('{}','{}')".format(_def, json.dumps(_args))
        res = self.insert(query=query)

    
    def get_jobs(self, limit=1):
        self.update(query="""UPDATE `queue` SET `state`='PENDING', `worker`={} WHERE `state`='TODO' LIMIT {}""".format(self.w_id, limit))
        return self.select(d=True, query="""SELECT * FROM `queue` WHERE `state`='PENDING' AND`worker`={}""".format(self.w_id))
        

    def start_job(self, j_id):
        res = self.update(query="""UPDATE `queue` SET `state`='AT_WORK' WHERE `id`={}""".format(j_id))
    
    def return_job(self, job_id, result):
        res = self.update(query="""UPDATE `queue` SET `state`='DONE', `result`='{}' WHERE `id` = {}""".format(json.dumps(result), job_id))
