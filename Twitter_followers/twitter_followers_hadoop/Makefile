hadoop.root=/Users/jieyusheng/download/hadoop-3.1.1
spark.root=/Users/jieyusheng/download/spark-2.3.1-bin-hadoop2.7

app.name=twitter_followers
jar.name=twitter_followers_hadoop-1.0.0-SNAPSHOT.jar
jar.file=target/${jar.name}
job.main=cs6240.hw1.twitter_followers
job.output=target/output
# master.local=local[*]
# master.pseudo=spark://172.16.10.180:7077
# maven.jar.name=twitter_followers_hadoop-1.0.0-SNAPSHOT.jar

local.input=target/classes/nodes.csv target/classes/edges.csv
hdfs.input=/user/${hdfs.user.name}/${app.name}
hdfs.user.name=jieyusheng
job.args=${hdfs.input}/nodes.csv ${hdfs.input}/edges.csv

ts=$(shell date +%m%d_%H%M%S)
aws.emr.release=emr-5.17.0
aws.region=us-east-1
aws.bucket.name=twitterfollowers
aws.subnet.id=subnet-6356553a
aws.input=big_input
aws.num.nodes=5
aws.output=hadoop_output_${aws.num.nodes}_$(ts)
aws.log.dir=hadoop_log_${aws.num.nodes}_$(ts)
aws.instance.type=m3.xlarge
#aws.instance.type=t2.medium

default: local

# Load data to HDFS
upload-input-hdfs:
	${hadoop.root}/bin/hdfs dfs -mkdir -p ${hdfs.input}
	${hadoop.root}/bin/hdfs dfs -put ${job.args} ${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

jar:
	mvn package -DskipTests=true

clean_local_output:	
	rm -rf ${job.output}

local: clean_local_output jar
	${hadoop.root}/bin/hadoop.cmd jar ${jar.file} ${job.main} ${job.args} ${job.output}

#pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
pseudo: jar 
	${hadoop.root}/bin/hadoop.cmd jar ${jar.file} ${job.main} ${job.args} ${job.output}
	#make download-output-hdfs



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
aws: jar delete-output-aws
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
