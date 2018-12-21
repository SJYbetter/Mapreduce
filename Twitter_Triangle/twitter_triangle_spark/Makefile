ifeq (${OS}, Windows_NT)
    hadoop.root=E:/apps/hadoop-3.1.1
    spark.root=E:/apps/spark-2.3.1-bin-hadoop2.7
    spark.submit=$(spark.root)/bin/spark-submit.cmd
else
   hadoop.root=/usr/local/hadoop-3.1.1
   spark.root=/usr/local/spark-2.3.2-bin-hadoop2.7
   spark.submit=$(spark.root)/bin/spark-submit
endif

app.name=twitter_triangle
jar.name=twitter_triangle_spark-1.0.0.jar
# job.main=cs6240.hw2.ReduceJoin
job.main=cs6240.hw2.ReplicatedJoinDS
#job.main =cs6240.hw2.ReplicatedJoinRDD

job.args=target/classes/edges.csv
job.output=target/output

# local.args=target/classes/edges.csv target/output
local.jar=target/${jar.name}
# local.edges=target/edges2.csv
local.edges=../report/edges2.csv
local.output=target/output
local.args=$(abspath $(local.edges)) $(abspath $(local.output)) 10000


master.local=local[*]
master.pseudo=spark://172.16.10.180:7077
maven.jar.name=twitter_followers_spark-1.0.0-SNAPSHOT.jar

#ts=$(shell date +%m%d_%H%M%S)

default: local

jar:
	mvn package -DskipTests=true

clean_local_target:
	rm -rf ${job.output}

local: clean_local_target #jar
	${spark.submit} --class ${job.main} --master ${master.local} ${local.jar} ${local.args}

#pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
pseudo: #jar
	${spark.submit} --class ${job.main} --master ${master.pseudo} --deploy-mode cluster ${local.jar} ${local.args}
	#make download-output-hdfs

