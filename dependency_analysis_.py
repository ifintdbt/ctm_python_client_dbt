import os
import re


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
        model_name = os.path.splitext(os.path.basename(model_file))[0]  # Remove .sql extension
        dependencies = extract_dependencies(model_file)
        dependency_graph[model_name] = dependencies
    return dependency_graph


def show_dependencies(models_folder):
    model_files = find_model_files(models_folder)
    dependency_graph = build_dependency_graph(model_files)

    for model, dependencies in dependency_graph.items():
        print(f"Model: {model}")
        print(f"Dependencies: {dependencies}")
        print()

    return dependency_graph
