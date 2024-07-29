import json
import os
from ctm_python_client.core.workflow import Workflow
from ctm_python_client.core.comm import Environment
from dependency_analysis import show_dependencies
from dbt_job import create_jobs_from_dependency_graph


def load_config(filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, filename)
    with open(config_path, 'r') as f:
        return json.load(f)

def main():
    # Load configurations
    dbt_config = load_config('Config/dbt_config.json')
    ctm_config = load_config('Config/ctm_environment.json')

    # Define environment and workflow
    env = Environment.create_onprem(ctm_config["url"], username=ctm_config["username"], password=ctm_config["password"])
    workflow = Workflow(env)

    # Define the path to the folder containing the model files
    models_folder = dbt_config["models_folder"]
    print(f"Using models folder: {models_folder}")

    if not os.path.exists(models_folder):
        print(f"Directory does not exist: {models_folder}")
        return

    print(f"Files in directory: {os.listdir(models_folder)}")

    # Build dependency graph from model files
    dependency_graph = show_dependencies(models_folder)
    print("Dependency Graph:", dependency_graph)

    # Create jobs based on the dependency graph
    create_jobs_from_dependency_graph(workflow, dependency_graph, dbt_config, ctm_config)

    # Print the workflow JSON
    try:
        workflow_json = workflow.dumps_json(indent=2)
        print("Workflow JSON:", workflow_json)
    except Exception as e:
        print(f"Failed to serialize workflow: {str(e)}")

    # Build, deploy, and run the workflow
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
