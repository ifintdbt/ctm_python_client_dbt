import os
import re
from collections import defaultdict

def show_dependencies(models_folder):
    # Dictionary to store dependencies
    dependency_graph = defaultdict(list)

    # Regular expression to find ref calls
    ref_pattern = re.compile(r"\{\{\s*ref\(['\"](.*?)['\"]\)\s*\}\}")

    # Iterate through model files
    for root, _, files in os.walk(models_folder):
        for file in files:
            if file.endswith(".sql"):
                model_name = file.replace(".sql", "")
                file_path = os.path.join(root, file)
                print(f"Analyzing file: {file_path}")  # Debugging statement
                with open(file_path, 'r') as f:
                    content = f.read()

                # Find all ref calls in the file
                matches = ref_pattern.findall(content)
                if matches:
                    for match in matches:
                        dependency_graph[model_name].append(match)
                        if match not in dependency_graph:
                            dependency_graph[match] = []  # Ensure all models are keys

                    print(f"Found dependency: {model_name} -> {match}")  # Debugging statement
                else:
                    # Ensure every model is initialized in the dependency graph
                    if model_name not in dependency_graph:
                        dependency_graph[model_name] = []

    return dict(dependency_graph)
