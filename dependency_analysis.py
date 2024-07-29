import os
import re
from collections import defaultdict

# This section will find the dependencies that exist in the model files - By looking into the ref functions which should return the relation.
# It is the core of the workflow's structure definitions.


def show_dependencies(models_folder):
    dependency_graph = defaultdict(list)
    ref_pattern = re.compile(r"\{\{\s*ref\(['\"](.*?)['\"]\)\s*\}\}")

    for root, _, files in os.walk(models_folder):
        for file in files:
            if file.endswith(".sql"):
                model_name = file.replace(".sql", "")
                file_path = os.path.join(root, file)
                print(f"Analyzing file: {file_path}")
                with open(file_path, 'r') as f:
                    content = f.read()

                matches = ref_pattern.findall(content)
                if matches:
                    for match in matches:
                        if match in dependency_graph or match != model_name:
                            dependency_graph[model_name].append(match)
                            print(f"Found dependency: {model_name} -> {match}")
                        else:
                            print(f"Warning: {model_name} has a reference to non-existent model {match}")

                if model_name not in dependency_graph:
                    dependency_graph[model_name] = []

    return dict(dependency_graph)
