ifeq (${OS}, Windows_NT)
    hadoop.root=/usr/local/hadoop-3.1.1
    spark.root=/usr/local/spark-2.3.1-bin-hadoop2.7
else
   hadoop.root=/usr/local/hadoop-3.1.1
   spark.root=/usr/local/spark-2.3.2-bin-hadoop2.7
endif

ts=$(shell date +%m%d_%H%M%S)

app.name=twitter_triangle_hadoop
jar.name=${app.name}-1.0.0.jar

job.main=cs6240.hw2.ReplicatedJoin
# job.main=cs6240.hw2.TriangleReduceJoin
# job.main=cs6240.hw2.GraphTriangleCount
# job.main=com.vertica.mr.graphs.TriangleCounter

# master.local=local[*]
# master.pseudo=spark://172.16.10.180:7077
# maven.jar.name=twitter_followers_hadoop-1.0.0-SNAPSHOT.jar

local.jar=target/${jar.name}
#local.input=target/classes/nodes.csv target/classes/edges.csv
#local.edges=E:/works/day1/dataset/Twitter-dataset/data/edges.csv
local.edges=target/classes/edges.csv
local.output=target/output

hdfs.prefix=/hw2/${app.name}
#hdfs.edges=${hdfs.prefix}/../edges.csv
hdfs.edges=/twitter_triangle_hadoop/edges.csv
hdfs.output=${hdfs.prefix}/output
hdfs.args=${hdfs.edges} ${hdfs.output} 1000

job.args=${hdfs.edges}/nodes.csv ${hdfs.edges}/edges.csv 1000


aws.emr.release=emr-5.17.0
aws.region=us-east-1
aws.bucket.name=twitterfollowers
aws.subnet.id=subnet-6356553a
aws.input=big_input
aws.num.nodes=10
aws.output=hadoop_output_${aws.num.nodes}_$(ts)
aws.log.dir=hadoop_log_${aws.num.nodes}_$(ts)
aws.instance.type=m3.xlarge
#aws.instance.type=t2.medium

# .ONESHELL:

default: local

jar:
	mvn package -DskipTests=true

clean_local_output:	
	rm -rf ${local.output}

local: clean_local_output jar
	${hadoop.root}/bin/hadoop.cmd jar ${local.jar} ${job.main} ${job.args} ${local.output}

# ************************** remote test hadoop env ********************************************
# Load data to HDFS
upload-input-hdfs:
	${hadoop.root}/bin/hdfs dfs -mkdir -p ${hdfs.prefix}
	${hadoop.root}/bin/hdfs dfs -put ${local.edges} ${hdfs.edges}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean_local_output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

#pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
pseudo: clean_local_output clean-hdfs-output
	${hadoop.root}/bin/hadoop jar ${local.jar} ${job.main} $(hdfs.args)
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 cp target/classes/nodes.csv s3://${aws.bucket.name}/${aws.input}
	aws s3 cp target/classes/edges.csv s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*" --include "${aws.log.dir}"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp target/${jar.name} s3://${aws.bucket.name}

# Main EMR launch.  jar upload-app-aws
aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "twitter followers hadoop ${aws.num.nodes}" \
		--release-label ${aws.emr.release} \
		--ec2-attributes KeyName=u1 \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark\
	    --steps '[{"Args":["${job.main}","s3://${aws.bucket.name}/${aws.input}/nodes.csv","s3://${aws.bucket.name}/${aws.input}/edges.csv","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
