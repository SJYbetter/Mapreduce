ifeq (${OS}, Windows_NT)
    hadoop.root=/usr/local/hadoop-3.1.1
    spark.root=/usr/local/spark-2.3.1-bin-hadoop2.7
else
   hadoop.root=/usr/local/hadoop-3.1.1
   spark.root=/Users/jieyusheng/spark-2.3.1-bin-hadoop2.7
endif

# app.name=actions_and_transformations

jar.name=graph_diameter-1.0.0.jar
job.main = DiameterDS

#job.main=cs6240.hw2.ReduceJoinRDD
#job.main=cs6240.hw2.ReplicatedJoinDS
#job.main=cs6240.hw2.ReplicatedJoinRDD

job.args=target/classes/edges.csv
job.output=target/output

local.jar=target/${jar.name}
local.edges=target/classes/edges2.csv
#local.edges=../report/edges2.csv
local.output=target/output
local.args=$(local.edges) 1 false $(abspath $(local.output))

spark.remote=spark://172.27.2.50:7077

#maven.jar.name=$(jar.name)

#ts=$(shell date +%m%d_%H%M%S)

include ../../Makefile.mk

all: local

#$(local.jar): $(java_files) $(scala_files)
#	mvn package -DskipTests=true

clean_local_target:
	rm -rf ${job.output}


local: spark_local

#pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 

pseudo: spark_pseudo
	# ${spark.submit} --class ${job.main} --master ${master.pseudo} --deploy-mode cluster ${local.jar} ${local.args}
	#make download-output-hdfs

