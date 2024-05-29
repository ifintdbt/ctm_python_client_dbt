from ctm_python_client.core.workflow import Workflow, WorkflowDefaults
from ctm_python_client.core.comm import Environment
from aapi import *

import attrs


@attrs.define
class AIDBTcore(AIJob):  # We derive from AIJob, the name of the class can be any valid python class name
    _type = AIJob.type_field(
        'dbt Core')  # We add a "type" field, with the same name you would see in the web interface.
    # the name in type IS important and should match the one seen in the Web Interface
    # in the Planning section

    dbtProjectPath = AIJob.field('dbt Project Path')
    VirtualEnvActivation = AIJob.field('Virtual Environment Activation Path')
    dbtcommand = AIJob.field('dbt Command')


# basic env
env = Environment.create_onprem("dba-tlv-wlad4r.bmc.com", username='emuser', password='empass')
workflow = Workflow(env)

# basic job
myJob = JobCommand('dbtJob', command='echo "Hello world!"', run_as='controlm')
workflow.add(myJob, inpath='ctmFolder')

# dbt job
dbtjob = AIDBTcore('DBT_WZ', connection_profile='DBT',
                   dbtProjectPath="C:\\Users\\dbauser\\dbt-env\\dbtcore",
                   VirtualEnvActivation="C:\\Users\\dbauser\\dbt-env\\Scripts\\activate",
                   dbtcommand="dbt run --select my_first_dbt_model")

workflow.add(dbtjob, inpath='ctmFolder')

print(workflow.dumps_json(indent=2))
exit

if workflow.build().is_ok():
    print('The workflow is valid!')
else:
    print(workflow.build().errors)

if workflow.deploy().is_ok():
    print('The workflow is deployed!')
if workflow.run():
    print('Run job')
else:
    print(workflow.deploy().errors)