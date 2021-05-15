DataEngineering HW #1

An AirFlow pipeline using docker-compose,  It reads from PostgreSQL and stores in MongoDB

The pipeline structure is as follows: (using the Jupyter lab, the user inserts a table to the PostgreSQL) >> 
(after the AirFlow Dag "postgressCSVJSONmongodbDAG" is triggred) >>
(Read the table from PostgreSQL) >> 
(transforms from csv to json) >>  
(save to MongoDB)

The following YouTube video is a demo of running the implementation:

https://youtu.be/HmnscXa8ZMY


the PPT has screenshots of the full running process of the pipeline.

The following are abbreviated steps for deploying the pipeline, a full detailed demonstration can be found in the YouTube video (~12 min) or the PPT:

1. Clone the repo.
2. echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
3. In the directory run 'docker-compose up airflow-init'
4. In the directory run ‘docker-compose up’
Once all containers are up, please use:

http://[      ]:8089/    to access  pgadmin
http://[      ]:8088/    to access  mongoexpress
http://[      ]:8886/    to access  jupyterlab
http://[      ]:8087/    to access  airflow 

5. short version of running the pipeline:
   a. using Jupyter lab push data to PostgreSQL
   b. migrate data from PostgreSQL to MongoDB by running "postgressCSVJSONmongodbDAG" DAG

the youtube video shows the full process using google cloud VM instance, to keep things short, the preperation of the VM is not provided, but can be, upon request. 

the pipeline comes with a csv data file (data.csv), using the Jupiter notebook it is possible to create further data but 
their would be a need to elevate the permissions, further can be found in the Jupiter notebook.
Note: Docker for ubuntu is provided in the utilities

oRefai