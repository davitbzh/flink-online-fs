import os
import requests
import argparse
import time
from json.decoder import JSONDecodeError

import hopsworks
from hopsworks.client import exceptions

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--hopsworks-url", type=str, help="Hopsworks url domain and port")
parser.add_argument("-p", "--project", type=str, help="The Hopsworks project name")
parser.add_argument("-jar", "--jar", type=str, help="The Flink job jar file")
parser.add_argument("-j", "--job", type=str, default='flinkcluster', help="The Hopsworks job name")
parser.add_argument("-m", "--main", type=str, help="The entry point to the application, file with main function")
parser.add_argument("-a", "--apikey", type=str, default="jobs.token", help="The file containing the API key to be used to connect to the project and submit the job")
parser.add_argument("-jargs", "--job-arguments", type=str, help="Flink job runtime arguments")
"""
parser.add_argument("action", type=str, nargs=argparse.REMAINDER, help="Job action")
parser.add_argument("-tm", "--task-managers", default="1", help="Number of Flink task managers")
parser.add_argument("-yjm", "--yarnjobManagerMemory", default="2048", help="Memory of the Flink job manager in MB")
parser.add_argument("-ytm", "--yarntaskManagerMemory", default="4096", help="Memory of the Flink task managers in MB")
parser.add_argument("-ys", "--yarnslots", default="1", help="Number of slots per TaskManager")
"""

args = parser.parse_args()
jar_path = args.jar
hopsworks_url = args.hopsworks_url
project_name = args.project
job_name = args.job
job_args = args.job_arguments
api_key = args.apikey.strip()
main_class = args.main

#------------
os.environ['HOPSWORKS_PROJECT'] = project_name
os.environ['HOPSWORKS_HOST'] = hopsworks_url
os.environ['HOPSWORKS_API_KEY'] = api_key
#------------

project = hopsworks.login()
jobs_api = project.get_jobs_api()

try:
    job = jobs_api.get_job(job_name)
except exceptions.RestAPIError as e:
    if e.response.json().get("errorCode", "") == 130009 and e.response.status_code == 404:
        flink_config = jobs_api.get_configuration("FLINK")
        flink_config['appPath'] = ""
        job = jobs_api.create_job(job_name, flink_config)

"""
# TODO
if 'stop' in args.action:
    print(job.stop_job(args.job))
    print("Stopped Flink cluster.")
    exit(0)
"""
executions = jobs_api.get_job(job_name).get_executions()
# Check if Flink job with this name is already running
if executions is None or len(executions) == 0:
    # run job
    job.run(await_termination=False)
    # Wait 90 seconds until runner is in status "RUNNING",
    wait = 90
    wait_count = 0
    execution = jobs_api.get_job(job_name).get_executions()[0]
    state = execution.state
    while wait_count < wait and state != "RUNNING":
        time.sleep(5)
        wait_count += 5
        execution = jobs_api.get_job(job_name).get_executions()[0]
        state = execution.state
    if state != "RUNNING":
        print("Flink cluster did not start, check job logs for details")
        exit(1)
    else:
        app_id = execution.app_id
        print("app_id: " + app_id)
        print("Flink cluster started successfully. Will now proceed to submit the job.")
else:
    app_id = executions[0].app_id
    print("Found Flink cluster with this name already running, will use it to submit the Flink job")

# url for flink cluster
base_url = "https://" + hopsworks_url + "/hopsworks-api/flinkmaster/" + app_id

# Upload Flink job jar to job manager
response = requests.post(
    base_url + "/jars/upload",
    verify=False,
    files={
        "jarfile": (
            os.path.basename(jar_path),
            open(jar_path, "rb"),
            "application/x-java-archive"
        )
    },
    headers={"Authorization": "Apikey " + api_key}
)

# Run job
jar_id = response.json()["filename"].split("/")[-1]
base_url += "/jars/" + jar_id + "/run?entry-class=" + main_class + "&program-args=" + job_args
print("Submitting job to: " + base_url)

response = requests.post(
    base_url,
    verify=False,
    headers={"Content-Type" : "application/json", "Authorization": "Apikey " + api_key}
)

try:
    response_object = response.json()
except JSONDecodeError:
    response_object = None

if (response.status_code // 100) != 2:
    if response_object:
        print(response_object)
    else:
        error_code, error_msg, user_msg = "", "", ""
    raise RestAPIError("Could not execute HTTP request (url: {}), server response: \n "
                       "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
        base_url, response.status_code, response.reason, error_code, error_msg, user_msg))
else:
    print("Flink job was submitted successfully, please check Hopsworks UI for progress.")