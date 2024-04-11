from queue import Queue
from threading import Thread, Event, Lock
import time
import os
import json

class ThreadPool:
    def __init__(self, output_file):
        self.num_threads = self.get_num_threads()
        self.output_file = output_file
        self.file_lock = Lock()
        self.tasks_queue = Queue()
        self.shutdown_event = Event()
        self.task_runners = [TaskRunner(self.tasks_queue, self.output_file, self.file_lock, self.shutdown_event) for _ in range(self.num_threads)]
        self.task_id = 0
        
    def get_num_threads(self):
        # Check if TP_NUM_OF_THREADS is defined, otherwise use hardware concurrency
        return int(os.getenv('TP_NUM_OF_THREADS', os.cpu_count()))
    
    def start(self):
        # create output file
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w') as f:
                f.write("{}")

        for task_runner in self.task_runners:
            task_runner.start()
    
    def submit_task(self, task, *args, **kwargs):
        if not self.shutdown_event.is_set():
            self.task_id += 1
            task_id = f"task_{self.task_id}"
            self.tasks_queue.put((task_id, task, args, kwargs))
            # Write new task to JSON file
            with open(self.output_file, 'r+') as f:
                tasks = json.load(f)
                tasks[task_id] = "running"
                f.seek(0)
                json.dump(tasks, f, indent=4)
            return task_id
        else:
            return "Processing has STOPPED!"

    def join(self):
        self.tasks_queue.join()
        self.graceful_shutdown()
    
    def graceful_shutdown(self):
        self.shutdown_event.set()
        for task_runner in self.task_runners:
            task_runner.join()

class TaskRunner(Thread):
    def __init__(self, tasks_queue, output_file, file_lock, shutdown_event):
        super().__init__()
        self.tasks_queue = tasks_queue
        self.output_file = output_file
        self.file_lock = file_lock
        self.shutdown_event = shutdown_event
        self.daemon = True

    def run(self):
        while not self.shutdown_event.is_set():
            if not self.tasks_queue.empty():
                # get task and execute
                task_id, task, args, kwargs = self.tasks_queue.get()
                result = task(*args, **kwargs)
                # write the result
                self.write_result(task_id, result)
                self.tasks_queue.task_done()
            else:
                # if queue is empty, wait before checking again for new tasks
                time.sleep(1)
    
    def write_result(self, task_id, result):
        with self.file_lock:
            # update JSON file when task is completed
            with open(self.output_file, 'r+') as f:
                tasks = json.load(f)
                tasks[task_id] = result
                f.seek(0)
                json.dump(tasks, f, indent=4)
