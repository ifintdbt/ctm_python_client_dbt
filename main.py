import os
import json
from ctm_python_client.core.workflow import Workflow
from ctm_python_client.core.comm import Environment
from dependency_analysis import show_dependencies
from dbt_job import create_jobs_from_dependency_graph

def load_config(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def main():
    # Load environment configuration
    env_config_path = os.path.join('config', 'ctm_environment.json')
    env_config = load_config(env_config_path)
    env = Environment.create_onprem(env_config["url"], username=env_config["username"], password=env_config["password"])

    # Load dbt configuration
    dbt_config_path = os.path.join('config', 'dbt_config.json')
    dbt_config = load_config(dbt_config_path)
    models_folder = dbt_config["models_folder"]

    workflow = Workflow(env)
    print(f"Using models folder: {models_folder}")

    if not os.path.exists(models_folder):
        print(f"Directory does not exist: {models_folder}")
        return

    print(f"Files in directory: {os.listdir(models_folder)}")

    dependency_graph = show_dependencies(models_folder)
    print("Dependency Graph:", dependency_graph)

    create_jobs_from_dependency_graph(workflow, dependency_graph)

    try:
        workflow_json = workflow.dumps_json(indent=2)
        print("Workflow JSON:", workflow_json)
    except Exception as e:
        print(f"Failed to serialize workflow: {str(e)}")

    build_response = workflow.build()
    if build_response.is_ok():
        print('The workflow is valid!')
    else:
        print("Build Errors:", build_response.errors)

    deploy_response = workflow.deploy()
    if deploy_response.is_ok():
        print('The workflow is deployed!')
    else:
        print("Deployment Errors:", deploy_response.errors)

if __name__ == "__main__":
    main()
