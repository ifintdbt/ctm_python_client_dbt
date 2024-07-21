import os
import re
from collections import defaultdict

def show_dependencies(models_folder):
    dependency_graph = defaultdict(list)
    ref_pattern = re.compile(r"\{\{\s*ref\(['\"](.*?)['\"]\)\s*\}\}")

    model_files = set()

    for root, _, files in os.walk(models_folder):
        for file in files:
            if file.endswith(".sql"):
                model_name = file.replace(".sql", "")
                model_files.add(model_name)

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
                        if match in model_files:
                            dependency_graph[model_name].append(match)
                            print(f"Found dependency: {model_name} -> {match}")
                        else:
                            print(f"Warning: {model_name} has a reference to non-existent model {match}")

    return dict(dependency_graph)
