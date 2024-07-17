import os
from ctm_python_client.core.workflow import Workflow
from ctm_python_client.core.comm import Environment
from dependency_analysis import show_dependencies
from dbt_job import create_jobs_from_dependency_graph

def main():
    # Define environment and workflow
    env = Environment.create_onprem("dba-tlv-wlad4r.bmc.com", username='emuser', password='empass')
    workflow = Workflow(env)

    # Define the path to the folder containing the model files
    models_folder = "C:\\Users\\dbauser\\dbt-env\\dbtcore\\models"
    print(f"Using models folder: {models_folder}")  # Debugging statement

    # Check if the path exists and list files
    if not os.path.exists(models_folder):
        print(f"Directory does not exist: {models_folder}")
        return

    print(f"Files in directory: {os.listdir(models_folder)}")

    # Build dependency graph from model files
    dependency_graph = show_dependencies(models_folder)
    print("Dependency Graph:", dependency_graph)  # Debugging statement

    # Create jobs based on the dependency graph
    create_jobs_from_dependency_graph(workflow, dependency_graph)

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
