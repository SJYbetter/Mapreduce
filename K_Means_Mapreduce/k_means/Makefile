ifeq (${OS}, Windows_NT)
    hadoop.root=/usr/local/hadoop-3.1.1
    spark.root=/usr/local/spark-2.3.1-bin-hadoop2.7
else
   hadoop.root=/usr/local/hadoop-3.1.1
   spark.root=/Users/jieyusheng/spark-2.3.1-bin-hadoop2.7
endif

app.name=k_means
jar.name=k_means-1.0.0.jar
#job.main = KMeansRDD
job.main = KMeansMR
#job.main=SetupMR

job.args=target/classes/edges.csv
job.output=target/output

# local.args=target/classes/edges.csv target/output
local.jar=target/$(jar.name)
# local.edges=target/edges2.csv
local.edges=../../testdata/test.edges.csv
local.output=target/output
local.args=$(abspath $(local.edges)) $(abspath $(local.output)) 2

#hdfs.output=/h4_big
hdfs.output=/h4_88
# hdfs.args=/datasets/edges.csv $(hdfs.output) 10
# hdfs.args=/h4_setup $(hdfs.output) 4 ${hdfs.output}/centroids
# bad 
# hdfs.args=/h4_setup $(hdfs.output) 4 /h4_setup/bad.csv
hdfs.args=/h4_setup $(hdfs.output) 4 random
# hdfs.args=/check $(hdfs.output) 4 random
#hdfs.args=/datasets/edges.csv $(hdfs.output) 10 random /h4_big/counts 

master.local=local[*]
master.pseudo=spark://172.16.10.180:7077
maven.jar.name=$(jar.name)

#ts=$(shell date +%m%d_%H%M%S)
aws.bucket.name=twitterfollowers
aws.emr.release=emr-5.17.0
aws.instance.type=m4.large

aws.log.dir=s3://${aws.bucket.name}/hw4/logs
aws.jar=s3://${aws.bucket.name}/$(jar.name)
aws.input=s3://${aws.bucket.name}/big_input/edges.csv
aws.output=s3://${aws.bucket.name}/hw4

aws.args="$(aws.input)","$(aws.output)","1","false"
default: local

include ../../Makefile.mk

#jar:
#	mvn package -DskipTests=true
#
#clean_local_target:
#	rm -rf ${job.output}

local: spark_local
#local: clean_local_target jar
#	${spark.submit} --class ${job.main} --master ${master.local} ${local.jar} ${local.args}

#pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
pseudo: #jar
	${spark.submit} --class ${job.main} --master ${master.pseudo} --deploy-mode cluster ${local.jar} ${local.args}
	#make download-output-hdfs


hadoop: hadoop_pseudo

aws_cluster_10:
	aws emr create-cluster \
		--name "K_means" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":10,"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		--ec2-attributes KeyName=u1 \
		--applications Name=Hadoop Name=Spark\
		--log-uri $(aws.log.dir)_10 \
		--use-default-roles \
		--enable-debugging 

aws_cluster_5:
	aws emr create-cluster \
		--name "K_means" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":5,"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		--ec2-attributes KeyName=u1 \
		--applications Name=Hadoop Name=Spark\
		--log-uri $(aws.log.dir) \
		--use-default-roles \
		--enable-debugging 
#		--steps Type=CUSTOM_JAR,Name="${job.main}",Jar="command-runner.jar",ActionOnFailure=CANCEL_AND_WAIT,Args=["spark-submit","--deploy-mode","cluster","--class","${job.main}","s3://${aws.bucket.name}/${jar.name}",$(aws.args)] \
#		--auto-terminate

s3_copy_to_hdfs:
	aws emr add-steps --cluster-id j-17CW72KXLPPIQ \
        --steps Name=LoadData,Jar=command-runner.jar,ActionOnFailure=CONTINUE,Type=CUSTOM_JAR,Args=s3-dist-cp,--src,$(aws.input),--dest,hdfs://edges.csv

aws_setup_job:
	aws emr add-steps --cluster-id j-3V5DPG2VSKZS2 \
		--steps Type=CUSTOM_JAR,Name=preprocessing,ActionOnFailure=CONTINUE,Jar=s3://${aws.bucket.name}/${jar.name},Args=SetupMR,$(aws.input),$(aws.output)/setup

aws_good_5:
	aws emr add-steps --cluster-id j-3V5DPG2VSKZS2 \
		--steps Type=CUSTOM_JAR,Name=k_means_good_5,ActionOnFailure=CONTINUE,Jar=s3://${aws.bucket.name}/${jar.name},Args=$(job.main),$(aws.output)/setup,$(aws.output)/k_means_good_5,4

aws_bad_5:
	aws s3 rm $(aws.output)/k_means_bad_5 --recursive 
	aws emr add-steps --cluster-id j-3V5DPG2VSKZS2 \
		--steps Type=CUSTOM_JAR,Name="k_means_bad_5",ActionOnFailure=CONTINUE,Jar="s3://${aws.bucket.name}/${jar.name}",Args=$(job.main),$(aws.output)/setup,$(aws.output)/k_means_bad_5,4,$(aws.output)/bad.csv

#        --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' 

aws_good_10:
	aws emr add-steps --cluster-id j-17CW72KXLPPIQ \
		--steps Type=CUSTOM_JAR,Name=k_means_good_10,ActionOnFailure=CONTINUE,Jar="s3://${aws.bucket.name}/${jar.name}",Args=$(job.main),$(aws.input),$(aws.output)/k_means_good_10,4,$(aws.output)/good.csv,$(aws.output)/counts



aws_bad_10:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "$(aws.output)/k_means_bad_10*"
	aws emr add-steps --cluster-id j-17CW72KXLPPIQ \
		--steps Type=CUSTOM_JAR,Name="k_means_bad_10",ActionOnFailure=CONTINUE,Jar="s3://${aws.bucket.name}/${jar.name}",Args=$(job.main),$(aws.input),$(aws.output)/k_means_bad_10,4,$(aws.output)/bad.csv,$(aws.output)/counts

testxx: 
	aws emr add-steps --cluster-id j-17CW72KXLPPIQ \
		--steps Type=CUSTOM_JAR,Name="test",ActionOnFailure=CONTINUE,Jar="s3://${aws.bucket.name}/${jar.name}",Args=S3TestJob,s3://twitterfollowers/hw4/k_means_bad_10/k_means_0
    