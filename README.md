# DataPipelinesWithAirflow
Repository for the project of the 4th course of the Data Engineering Nanodegree of Udacity.

### Installation and setup
This project requires you to install into your python environment or workspace the included dpa package.
- Go to the root of the repository and execute this command.
```
pip install -e .
```

- Modify your airflow.cfg file. The dags_folder variable should be the absolute path to the dpa package folder.

- Add the connections and variables for the project in Airflow. The redshift conn should be named "redshift" and the aws
one should be name "aws_credentials".

- Please set an Airflow Variable called "redshift_iam_arn" with the ARN value of the redshift role.

- Once set, you can run the airflow server, activate the sparkify DAG, and run it.