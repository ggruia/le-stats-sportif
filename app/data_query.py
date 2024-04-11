import json

class DataQuery:
    def __init__(self, file):
        self.file = file

    def num_jobs(self):
        with open(self.file, 'r+') as f:
            tasks = json.load(f)
            return len([1 for (_, v) in tasks.items() if v == "running"])

    def jobs(self):
        with open(self.file, 'r+') as f:
            tasks = json.load(f)
            jobs = [{job_id: status} if status == "running" else {job_id: "done"} for job_id, status in tasks.items()]
            return {"status": "done", "data": jobs}
    
    def job_result(self, job_id):
        with open(self.file, 'r+') as f:
            tasks = json.load(f)
            value = tasks.get(job_id, "error")
            if value == "error":
                return {"status": "error", "reason": "Invalid job_id"}
            elif value == "running":
                return {"status": "running"}
            else:
                return {"status": "done", "data": value}