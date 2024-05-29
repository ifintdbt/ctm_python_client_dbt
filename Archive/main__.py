import os
import re

from ctm_python_client.core import workflow

#import dbt_script_events_


def find_model_files(folder_path):
    model_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".sql"):
                model_files.append(os.path.join(root, file))
    return model_files


def extract_dependencies(model_file):
    dependencies = set()
    with open(model_file, 'r') as f:
        content = f.read()
        ref_pattern = re.compile(r'{{\s*ref\(\'([^\']+)\'.*?}}')
        matches = ref_pattern.findall(content)
        dependencies.update(matches)
    return dependencies


def build_dependency_graph(model_files):
    dependency_graph = {}
    for model_file in model_files:
        model_name = os.path.basename(model_file)
        dependencies = extract_dependencies(model_file)
        dependency_graph[model_name] = dependencies
    return dependency_graph


def show_dependencies():
    models_folder = "C:\Python_Excercises\PythonCTM\example"
    model_files = find_model_files(models_folder)
    dependency_graph = build_dependency_graph(model_files)

    for model, dependencies in dependency_graph.items():
        print(f"Model: {model}")
        print(f"Dependencies: {dependencies}")
        print()

    #find_dep = [ref_dep for ref_dep in dependency_graph if "dbt_model" in ref_dep]
    #print(f" Dependencies: {find_dep}")

    return dependency_graph


def create_controlm_jobs_graph():
    models_folder = "C:\Python_Excercises\PythonCTM\example"
    model_files = find_model_files(models_folder)
    dependency_graph = build_dependency_graph(model_files)

    for model, dependencies in dependency_graph.items():

        if dependencies == set():
            dbt_script_events.dbtjob = dbt_script_events.AIDBTcore('DBT_WZ', connection_profile='DBT',
                                                                   dbtProjectPath="C:\\Users\\dbauser\\dbt-env\\dbtcore",
                                                                   VirtualEnvActivation="C:\\Users\\dbauser\\dbt-env\\Scripts\\activate",
                                                                   dbtcommand=f"dbt run --select {model}")
            workflow.add(dbt_script_events.dbtjob, inpath='ctmFolder')


if __name__ == "__main__":
    results = show_dependencies()
    print (f"results: {results}")
