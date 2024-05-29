from aapi.job import *
from ctm_python_client.core.workflow import *
from ctm_python_client.core.comm import *
from ctm_python_client.core.credential import *
from ctm_python_client.core.monitoring import *


def main():
    my_environment = Environment('https://dba-tlv-wkiuk6:8443/automation-api', username='emuser', password='empass', mode=EnvironmentMode.ONPREM)
    print("The env: " + str(my_environment))
    workflow = Workflow(my_environment)

    # Create job with the "Run As" user specified
    myJob = JobCommand('MyFirstJob', command='echo "Hello world!"', run_as='emuser')

    # Add the job to the workflow
    workflow.add(myJob, inpath='MyFirstFolder')

    print("Workflow submitted successfully!")

    if workflow.build().is_ok():
        print('The workflow is valid!')

    if workflow.deploy().is_ok():
        print('The workflow was deployed to Control-M!')

    run = workflow.run()

    run.print_output('MyFirstJob')

    runm = RunMonitor(run_id='000d1', aapiclient=OnPremAAPIClient)
    runm.get_jobid(job_name='Snowflake_Job_2')

    runm.rerun_job(self=RunMonitor, job_name='Snowflake_Job_2')


if __name__ == '__main__':
    main()
