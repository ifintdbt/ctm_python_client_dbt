import attrs
from ctm_python_client.core.workflow import Workflow
from ctm_python_client.core.comm import Environment
from aapi import *

@attrs.define
class AIDBTcore(AIJob):
    _type = AIJob.type_field('dbt Core')
    dbtProjectPath = AIJob.field('dbt Project Path')
    VirtualEnvActivation = AIJob.field('Virtual Environment Activation Path')
    dbtcommand = AIJob.field('dbt Command')

def create_dbt_job(workflow, job_name, connection_profile, dbt_project_path, virtual_env_activation, dbt_command,
                   event_name=None, wait_event_name=None, when=None):
    dbtjob = AIDBTcore(job_name, connection_profile=connection_profile,
                       dbtProjectPath=dbt_project_path,
                       VirtualEnvActivation=virtual_env_activation,
                       dbtcommand=dbt_command)

    if wait_event_name:
        waitForEventList = WaitForEvents([Event(event=wait_event_name)])
        dbtjob.event_list.append(waitForEventList)

    if event_name:
        addEventListObject = AddEvents([EventOutAdd(event=event_name)])
        dbtjob.event_list.append(addEventListObject)

    if when:
        dbtjob.when = when

    workflow.add(dbtjob, inpath='ctmFolder')
    return dbtjob

def create_jobs_from_dependency_graph(workflow, dependency_graph):
    job_objects = {}

    def create_job_recursive(model):
        if model in job_objects:
            return job_objects[model]

        dependencies = dependency_graph[model]
        print(f'dep: {dependencies}')

        when_schedule = Job.When(
            week_days=['NONE'],
            month_days=['NONE'],
            week_days_calendar='MatiCal'
        )

        if dependencies:
            # Create jobs for dependencies first
            for dependency in dependencies:
                create_job_recursive(dependency)

            wait_event_name = f"{list(dependencies)[0]}_done"
            dbtjob = create_dbt_job(
                workflow,
                job_name=model,
                connection_profile='DBT',
                dbt_project_path="C:\\Users\\dbauser\\dbt-env\\my_bank_project",
                virtual_env_activation="C:\\Users\\dbauser\\dbt-env\\Scripts\\activate",
                dbt_command=f"dbt run --select {model}",
                wait_event_name=wait_event_name,
                event_name=f"{model}_done" if any(model in deps for deps in dependency_graph.values()) else None,
                when=when_schedule
            )
        elif not dependencies:
            dbtjob = create_dbt_job(
                workflow,
                job_name=model,
                connection_profile='DBT',
                dbt_project_path="C:\\Users\\dbauser\\dbt-env\\my_bank_project",
                virtual_env_activation="C:\\Users\\dbauser\\dbt-env\\Scripts\\activate",
                dbt_command=f"dbt run --select {model}",
                event_name=f"{model}_done",
                when=when_schedule
            )

        job_objects[model] = dbtjob
        return dbtjob

    for model in dependency_graph:
        create_job_recursive(model)
