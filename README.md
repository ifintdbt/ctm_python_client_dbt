<h1> Control-M Python Client Scripts Dynamic Orchestration of dbt models with Control-M </h1>

Control-M's Python Client allows Python developers to have seamless interaction with Control-M. 
For running the automated management and coordination of complex dbt processes and workflows at the time of model development, 
we can leverage the Control-M python client together with a code repository solution, for allowing dbt models to be orchestrated in workflows that are dynamically defined.
 
Control-M’s scheduling capabilities, combined with the Control-M Python Client, enable the scheduling of dbt jobs at regular intervals or in response to specific events. This ensures that data models are updated and maintained automatically.
By having the models automatically included into a workflow and the dependencies built based on the model internal references, this will allow an automated CI/CD process from the point the dbt developer commits his code.
This library provides a sample Control-M Python Client project that integrates dbt Core with Control-M for workflow automation. The integration facilitates the automatic generation and deployment of dbt jobs based on the dependencies defined in the dbt models.

<h2>Configuration</h2>

**1. dbt and Control-M Configurations**

In the Config folder there are two JSON files the scripts use for accessing the dbt project:

**dbt_config.json** <br>

•	The dbt project folder the Control-M job should point to <br><br>

•	Virtual Environment Activiation - For enabling the dbt commands executions <br><br>

•	models folder - For retrieving the set of models and dependencies the scripts should look into for creating the workflow in Control-M <br><br>

{ "models_folder": "C:\Path\To\Models\Folder", "dbt_project_path": "C:\Path\To\dbt\Project\Folder", "virtual_env_activation": "C:\Path\To\dbt-env\Scripts\activate" }

**ctm_environment.json**

•	The Control-M environment the Python Client should point to for deploying the workflow <br><br>
•	Control-M's Username and Password <br><br>
•	Job Connection Profile <br><br>
•	The Control-M workflow name <br><br>
•	Week Days Calender, for adding a job attribute fo an automated environment ordering schedule function (Optional) <br><br>
{ "url": "hostname.controlm_env.com", "username": "controlmuser", "password": "controlmpassword", "connection_profile": "ConnectionProfileName", "week_days_calendar": "CalName", "inpath": "ctmFolder" }

**2. **CI/CD Pipeline Configuration** - It is recommended to have a pipeline automatically executed on every change commited to the dbt Models folder, for having the Control-M workflow dynamically orchestrated.
This pipeline should execute the main.py script.
Jenkins, Azure DevOps and other CI/CD tools can be used to acheive this.
