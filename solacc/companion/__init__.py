# Databricks notebook source
from dbacademy.clients.dbrest import from_notebook
from dbacademy.dbgems import get_notebook_dir
from dbacademy.common.cloud import Cloud
# from dbruntime.display import displayHTML
import hashlib
import time
import copy
import json

class NotebookSolutionCompanion():
  """
  A class to provision companion assets for a notebook-based solution, includingn job, cluster(s), DLT pipeline(s) and DBSQL dashboard(s)
  """
  
  def __init__(self):
    self.solution_code_name = get_notebook_dir().split('/')[-1]
    self.cloud = Cloud.current_cloud()
    self.solacc_path = get_notebook_dir()
    hash_code = hashlib.sha256(self.solacc_path.encode()).hexdigest()
    self.job_name = f"[RUNNER] {self.solution_code_name} | {hash_code}" # use hash to differentiate solutions deployed to different paths
    self.client = from_notebook()

    print(f"Solution Companion for {self.solution_code_name} is initialized")
    print("cloud: ", self.cloud)
    print("solacc_path: ", self.solacc_path)
    print("job_name: ", self.job_name)
    print("client endpoint: ", self.client.endpoint)

  @staticmethod
  def convert_job_cluster_to_cluster(job_cluster_params):
    params = job_cluster_params["new_cluster"]
    params["cluster_name"] = f"""{job_cluster_params["job_cluster_key"]}"""
    params["autotermination_minutes"] = 15 # adding a default autotermination as best practice
    return params

  def create_or_update_job_by_name(self, params):
    """Look up the companion job by name and resets it with the given param and return job id; create a new job if a job with that name does not exist"""
    jobs = self.client.jobs().list()
    jobs_matched = list(filter(lambda job: job["settings"]["name"] == params["name"], jobs)) 
    assert len(jobs_matched) <= 1, f"""Two jobs with the same name {params["name"]} exist; please manually inspect them to make sure solacc job names are unique"""
    job_id = jobs_matched[0]["job_id"] if len(jobs_matched)  == 1 else None
    if job_id: 
      reset_params = {"job_id": job_id,
                     "new_settings": params}
      json_response = self.client.api("POST", f"{self.client.endpoint}/api/2.1/jobs/reset", reset_params)
      print("json_response: ", json.dumps(json_response, indent=2))
      # json_response = self.client.execute_post_json(f"{self.client.endpoint}/api/2.1/jobs/reset", reset_params) # returns {} if status is 200
      # assert json_response == {}, "Job reset returned non-200 status"
      # displayHTML(f"""Reset the <a href="/#job/{job_id}/tasks" target="_blank">{params["name"]}</a> job to original definition""")
    else:
      json_response = self.client.execute_post_json(f"{self.client.endpoint}/api/2.1/jobs/create", params)
      job_id = json_response["job_id"]
      # displayHTML(f"""Created <a href="/#job/{job_id}/tasks" target="_blank">{params["name"]}</a> job""")
    return job_id
  
  def create_or_update_cluster_by_name(self, params):
      """Look up a companion cluster by name and edit with the given param and return cluster id; create a new cluster if a cluster with that name does not exist"""
      
      def edit_cluster(client, cluster_id, params):
        """Wait for a cluster to be in editable states and edit it to the specified params"""
        cluster_state = client.execute_get_json(f"{client.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")["state"]
        while cluster_state not in ("RUNNING", "TERMINATED"): # cluster edit only works in these states; all other states will eventually turn into those two, so we wait and try later
          time.sleep(30) 
          cluster_state = client.execute_get_json(f"{client.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")["state"]
        json_response = client.execute_post_json(f"{client.endpoint}/api/2.0/clusters/edit", params) # returns {} if status is 200
        assert json_response == {}, "Cluster edit returned non-200 status"
      
      clusters = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/clusters/list")["clusters"]
      clusters_matched = list(filter(lambda cluster: params["cluster_name"] == cluster["cluster_name"], clusters))
      cluster_id = clusters_matched[0]["cluster_id"] if len(clusters_matched) == 1 else None
      if cluster_id: 
        params["cluster_id"] = cluster_id
        edit_cluster(self.client, cluster_id, params)
        # displayHTML(f"""Reset the <a href="/#setting/clusters/{cluster_id}/configuration" target="_blank">{params["cluster_name"]}</a> cluster to original definition""")
        
      else:
        json_response = self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/clusters/create", params)
        cluster_id = json_response["cluster_id"]
        # displayHTML(f"""Created <a href="/#setting/clusters/{cluster_id}/configuration" target="_blank">{params["cluster_name"]}</a> cluster""")
      return cluster_id

  @staticmethod
  def customize_job_json(input_json, job_name, solacc_path, cloud):
    input_json["name"] = job_name

    for i, _ in enumerate(input_json["tasks"]):
      if "notebook_task" in input_json["tasks"][i]:
        notebook_name = input_json["tasks"][i]["notebook_task"]['notebook_path']
        input_json["tasks"][i]["notebook_task"]['notebook_path'] = solacc_path + "/" + notebook_name
        
    if "job_clusters" in input_json:
      for j, _ in enumerate(input_json["job_clusters"]):
        if "new_cluster" in input_json["job_clusters"][j]:
          node_type_id_dict = input_json["job_clusters"][j]["new_cluster"]["node_type_id"]
          input_json["job_clusters"][j]["new_cluster"]["node_type_id"] = node_type_id_dict[cloud.value]
          if cloud == Cloud.AWS: 
            input_json["job_clusters"][j]["new_cluster"]["aws_attributes"] = {
                              "availability": "ON_DEMAND",
                              "zone_id": "auto"
                          }
          if cloud == Cloud.MSA: 
            input_json["job_clusters"][j]["new_cluster"]["azure_attributes"] = {
                              "availability": "ON_DEMAND_AZURE",
                              "zone_id": "auto"
                          }
          if cloud == Cloud.GCP: 
            input_json["job_clusters"][j]["new_cluster"]["gcp_attributes"] = {
                              "use_preemptible_executors": False
                          }
    return input_json
  
  def deploy_compute(self, input_json, run_job=False, wait=0):
    print(f"Deploying {self.job_name} job")

    self.job_input_json = copy.deepcopy(input_json)
    self.job_params = self.customize_job_json(self.job_input_json, self.job_name, self.solacc_path, self.cloud)
    print(json.dumps(self.job_params, indent=2))

    self.job_id = self.create_or_update_job_by_name(self.job_params)
    print(f"Job {self.job_id} is created")
    time.sleep(wait) # adding wait (seconds) to allow time for JSL cluster configuration using Partner Connect to complete
    # if not run_job: # if we don't run job, create interactive cluster
    #   if "job_clusters" in self.job_params:
    #     for job_cluster_params in self.job_params["job_clusters"]:
    #       _ = self.create_or_update_cluster_by_name(self.convert_job_cluster_to_cluster(job_cluster_params))
    # else:
    #   self.run_job()

  def run_job(self):
    self.run_id = self.client.jobs().run_now(self.job_id)["run_id"]
    response = self.client.runs().wait_for(self.run_id)
    
    # print info about result state
    self.test_result_state= response['state'].get('result_state', None)
    self.life_cycle_state = response['state'].get('life_cycle_state', None)
    
    print("-" * 80)
    print(f"Job #{self.job_id}-{self.run_id} is {self.life_cycle_state} - {self.test_result_state}")
    assert self.test_result_state == "SUCCESS", f"Job Run failed: please investigate the job run in this current workspace at #job/{self.job_id}/run/{self.run_id}" 
