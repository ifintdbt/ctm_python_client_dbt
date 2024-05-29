class Job:
    def __init__(self, name):
        self.name = name
        self.parent = None
        self.children = []

# Define the dictionary
my_dict = {'my_first_dbt_model.sql': set(),
           'my_second_dbt_model.sql': {'my_first_dbt_model.sql'},
           'my_third_dbt_model.sql': {'my_second_dbt_model.sql'}}

# Create a dictionary to store jobs
jobs = {}

# Iterate over the dictionary
for model, dependencies in my_dict.items():
    if dependencies == set():  # If model has an empty set of dependencies (parent)
        jobs[model] = Job(model)  # Create a job object
    else:  # If model has dependencies (child)
        if model not in jobs:  # Create a job object if not already created
            jobs[model] = Job(model)
        # Associate each dependency with the job
        for dependency in dependencies:
            if dependency in jobs:  # If dependency already created as a job
                jobs[dependency].children.append(jobs[model])  # Add current job as child of dependency
                jobs[model].parent = jobs[dependency]  # Set dependency as parent of current job
            else:  # If dependency not yet created as a job
                jobs[dependency] = Job(dependency)  # Create dependency job
                jobs[dependency].children.append(jobs[model])  # Add current job as child of dependency
                jobs[model].parent = jobs[dependency]  # Set dependency as parent of current job


# Print the job hierarchy
def print_job_hierarchy(job, printed_jobs=None, indent=""):
    if printed_jobs is None:
        printed_jobs = set()

    if job.parent is None:  # If job has no parent, it's a root job
        if job.name not in printed_jobs:
            printed_jobs.add(job.name)
            print("Parents:")
    else:
        if job.parent.name not in printed_jobs:
            printed_jobs.add(job.parent.name)
            print(f"  - Job: {job.parent.name}")
            print(f"    Children: {job.name}")
    if job.children:  # If job has children, recursively print their hierarchy
        for child in job.children:
            print_job_hierarchy(child, printed_jobs, indent)


print("Job Hierarchy:")
for job in jobs.values():
    print_job_hierarchy(job)
