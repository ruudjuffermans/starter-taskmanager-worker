from db import Db

class WorkerModel(Db):
    def __init__(self, w_id):
        self.w_id = w_id
        super(WorkerModel, self).__init__(w_id)
        self._sync()
        if self.state["state"] == "RUNNING":
            self.update(query="""UPDATE `worker` SET `state`='INTERRUPTED' WHERE `id`={};""".format(self.w_id))
        else:
            self._initiate_start()
    
    def _initiate_start(self):
        self.update(query="""UPDATE `worker` SET `state`='INITIALIZING' WHERE `id`={};""".format(self.w_id))
        self.initialize()

    def _start(self):
        self.update(query="""UPDATE `worker` SET `state`='RUNNING' WHERE `id`={};""".format(self.w_id))

    def _initiate_stop(self):
        self.update(query="""UPDATE `worker` SET `state`='STOPPING' WHERE `id`={};""".format(self.w_id))

    def _stop(self):
        self.update(query="""UPDATE `worker` SET `state`='DOWN' WHERE `id`={};""".format(self.w_id))

    def _sync(self):
        self.state = self.single(query="""SELECT * FROM `worker` WHERE `id` = {};""".format(self.w_id))

