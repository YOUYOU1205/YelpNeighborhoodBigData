# start spark cluster and ssh into Master Node
peg start spark-cluster
peg fetch spark-cluster
peg service spark-cluster spark start
peg service spark-cluster hadoop start
peg ssh spark-cluster 1
# Ingest Yelp json data from s3 to HDFS
aws s3 sync s3://yelp-dump /home/ubuntu/s3
hdfs dfs -mkdir /user
hdfs dfs -copyFromLocal business.json /user/business.json
hdfs dfs -copyFromLocal checkin.json /user/checkin.json
# Install jdbc drive in HDFS
wget https://jdbc.postgresql.org/download/postgresql-42.2.8.jar
hdfs dfs -copyFromLocal postgresql-42.2.8.jar /user/postgresql-42.2.8.jar
spark-submit --master spark://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:7077  --jars postgresql-42.2.8.jar --driver-class-path=hdfs://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:9000/user/postgresql-42.2.8.jar --conf spark.executor.extraClassPath=hdfs://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:9000/user/postgresql-42.2.8.jar final.py