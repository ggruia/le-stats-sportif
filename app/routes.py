from app import webserver, tasks_runner, data_ingestor, data_query
from flask import request, jsonify

import os
import json


# Jobs
@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    data = request.json
    question = data['question']

    return tasks_runner.submit_task(data_ingestor.states_mean, question)

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    data = request.json
    question = data['question']
    state = data['state']

    return tasks_runner.submit_task(data_ingestor.state_mean, question, state)

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    data = request.json
    question = data['question']

    return tasks_runner.submit_task(data_ingestor.best5, question)

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    data = request.json
    question = data['question']

    return tasks_runner.submit_task(data_ingestor.worst5, question)

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    data = request.json
    question = data['question']

    return tasks_runner.submit_task(data_ingestor.global_mean, question)

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    data = request.json
    question = data['question']

    return tasks_runner.submit_task(data_ingestor.diff_from_mean, question)

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    data = request.json
    question = data['question']
    state = data['state']

    return tasks_runner.submit_task(data_ingestor.state_diff_from_mean, question, state)

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    data = request.json
    question = data['question']

    return tasks_runner.submit_task(data_ingestor.mean_by_category, question)

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    data = request.json
    question = data['question']
    state = data['state']

    return tasks_runner.submit_task(data_ingestor.state_mean_by_category, question, state)


# Queries
@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    return str(data_query.num_jobs())

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    return jsonify(data_query.jobs())

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def job_result(job_id):
    return jsonify(data_query.job_result(job_id))


# Graceful Shutdown
@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    tasks_runner.graceful_shutdown()
    return "notified TERM"

# Index
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
