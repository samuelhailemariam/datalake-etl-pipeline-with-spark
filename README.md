## Data Lake with Spark and AWS EMR

#### Summary
This project builds an ETL pipeline that loads data from S3, process the data into anlaytics table using Spark hosted on AWS EMR cluster.

#### Details
A music streaming startup, Sparkify, has a requirement to move their data warehouse to a data lake. The data currently resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project attempts to build an ETL pipeline that loads data from S3, process the data into anlaytics table using Spark, and load the data back into S3 as a set of dimensional tables, which will enable the analytics team to gather insights into the songs their users are listening to. The Spark process is run on top of AWS EMR cluster.

#### Project Structure
Below is the project structure:
- **/data**  - contails zip files for data exploration.
- **dl.cfg** - configuration file for AWS credentials.
- **etl.py** - the ETL pipeline file.

#### How to run

1. Configure ***dl.cfg*** with the correct AWS credentials.
2. Connect to the master node using ssh and copy ***dl.cfg*** and ***etl.py*** to the master node.
3. Execute the following command:

	- `/usr/bin/spark-submit --master yarn ./etl.py`