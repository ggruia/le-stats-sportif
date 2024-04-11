from flask import Flask
from app.data_ingestor import DataIngestor
from app.data_query import DataQuery
from app.task_runner import ThreadPool
import os

webserver = Flask(__name__)
webserver.json.sort_keys = False
webserver.job_counter = 1

output_file = 'output.json'
if os.path.exists(output_file):
  os.remove(output_file)

data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
data_query = DataQuery(output_file)

tasks_runner = ThreadPool(output_file)
tasks_runner.start()

from app import routes
