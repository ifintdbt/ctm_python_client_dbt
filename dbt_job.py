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

def create_jobs_from_dependency_graph(workflow, dependency_graph, config):
    job_objects = {}
    dbt_project_path = config.get("dbt_project_path")
    virtual_env_activation = config.get("virtual_env_activation")

    def create_job_recursive(model):
        if model in job_objects:
            return job_objects[model]

        dependencies = dependency_graph[model]
        when_schedule = Job.When(
            week_days=['NONE'],
            month_days=['NONE'],
            week_days_calendar='MatiCal'
        )

        if dependencies:
            # Create jobs for dependencies first
            for dependency in dependencies:
                create_job_recursive(dependency)

            # Wait for all dependencies to complete
            wait_event_names = [f"{dep}_done" for dep in dependencies]
            dbtjob = create_dbt_job(
                workflow,
                job_name=model,
                connection_profile='DBT',
                dbt_project_path=dbt_project_path,
                virtual_env_activation=virtual_env_activation,
                dbt_command=f"dbt run --select {model}",
                wait_event_name=None,  # No immediate event to wait for
                event_name=f"{model}_done",  # Event triggered after completion
                when=when_schedule
            )

            # Add wait events for each dependency
            for wait_event_name in wait_event_names:
                waitForEventList = WaitForEvents([Event(event=wait_event_name)])
                dbtjob.event_list.append(waitForEventList)

        else:
            # If no dependencies, create job with no wait events
            dbtjob = create_dbt_job(
                workflow,
                job_name=model,
                connection_profile='DBT',
                dbt_project_path=dbt_project_path,
                virtual_env_activation=virtual_env_activation,
                dbt_command=f"dbt run --select {model}",
                event_name=f"{model}_done",
                when=when_schedule
            )

        job_objects[model] = dbtjob
        return dbtjob

    # Create jobs for each model in the dependency graph
    for model in dependency_graph:
        create_job_recursive(model)
