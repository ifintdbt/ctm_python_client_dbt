import json
import os
from ctm_python_client.core.workflow import Workflow
from ctm_python_client.core.comm import Environment
from dependency_analysis import show_dependencies
from dbt_job import create_jobs_from_dependency_graph

#This section defines the function for reading the data out of the JSON configurations files
def load_config(filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, filename)
    with open(config_path, 'r') as f:
        return json.load(f)

def main():
    # There are two files that exist in the Config directory for loading the configurations that will point to the dbt project and ControlM
    dbt_config = load_config('Config/dbt_config.json')
    ctm_config = load_config('Config/ctm_environment.json')

    # Defines the CTM environment and workflow
    env = Environment.create_onprem(ctm_config["url"], username=ctm_config["username"], password=ctm_config["password"])
    workflow = Workflow(env)

    # Defines the path to the folder containing the dbt model files
    models_folder = dbt_config["models_folder"]
    print(f"Using models folder: {models_folder}")

    if not os.path.exists(models_folder):
        print(f"Directory does not exist: {models_folder}")
        return

    print(f"Files in directory: {os.listdir(models_folder)}")

    # Builds a dependency graph structure from the models folder in the dbt project
    dependency_graph = show_dependencies(models_folder)
    print("Dependency Graph:", dependency_graph)

    # Creates the jobs based on the dependency graph
    create_jobs_from_dependency_graph(workflow, dependency_graph, dbt_config, ctm_config)

    # Prints the workflow JSON structure into the logs
    try:
        workflow_json = workflow.dumps_json(indent=2)
        print("Workflow JSON:", workflow_json)
    except Exception as e:
        print(f"Failed to serialize workflow: {str(e)}")

    # Build and deploys the workflow. Ordering/Running the workflow env can also be added to this section.
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
