from ctm_python_client.core.workflow import Workflow
from ctm_python_client.core.comm import Environment
from dependency_analysis import show_dependencies
from dbt_jobs import create_jobs_from_dependency_graph


def main():
    # Define environment and workflow
    env = Environment.create_onprem("dba-tlv-wlad4r.bmc.com", username='emuser', password='empass')
    workflow = Workflow(env)

    # Define the path to the folder containing the model files
    models_folder = "C:\\Projects\\PythonCTM\\example"

    # Build dependency graph from model files
    dependency_graph = show_dependencies(models_folder)

    # Create jobs based on the dependency graph
    create_jobs_from_dependency_graph(workflow, dependency_graph)

    # Print the workflow JSON
    print(workflow.dumps_json(indent=2))

    # Build, deploy, and run the workflow
    if workflow.build().is_ok():
        print('The workflow is valid!')
    else:
        print(workflow.build().errors)

    if workflow.deploy().is_ok():
        print('The workflow is deployed!')
    #if workflow.run():
     #   print('Run job')
    else:
        print(workflow.deploy().errors)


if __name__ == "__main__":
    main()
