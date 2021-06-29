#!/bin/bash

download_spark () {
	cd ~
	wget https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
	tar -xzf spark-2.4.4-bin-hadoop2.7.tgz
	
}

configure_spark () {

	echo "export SPARK_HOME=/home/user/spark-2.4.4-bin-hadoop2.7" >> ~/.bashrc
	echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
	echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
	echo "alias start-all.sh='\$SPARK_HOME/sbin/start-all.sh'" >> ~/.bashrc
	echo "alias stop-all.sh='\$SPARK_HOME/sbin/stop-all.sh'" >> ~/.bashrc

	source ~/.bashrc

	cd /home/user/spark-2.4.4-bin-hadoop2.7/conf

	cp spark-env.sh.template spark-env.sh
	echo "SPARK_WORKER_CORES=2" >> spark-env.sh
	echo "SPARK_WORKER_MEMORY=3g" >> spark-env.sh

	cp spark-defaults.conf.template spark-defaults.conf
	echo "spark.master\t\tspark://master:7077" >> spark-defaults.conf
	echo "spark.submit.deployMode\t\tclient" >> spark-defaults.conf
	echo "spark.executor.instances\t\t2" >> spark-defaults.conf
	echo "spark.executor.cores\t\t2" >> spark-defaults.conf
	echo "spark.executor.memory\t\t3g" >> spark-defaults.conf
	echo "spark.driver.memory\t\t512m" >> spark-defaults.conf
	
	echo "master" > slaves
	echo "slave" >> slaves
}


echo "STARTING DOWNLOAD ON MASTER"
download_spark

echo "STARTING DOWNLOAD ON SLAVE"
ssh user@slave "$(typeset -f download_spark); download_spark"

echo "STARTING HADOOP CONFIGURE ON MASTER"
configure_spark

echo "STARTING HADOOP CONFIGURE ON SLAVE"
ssh user@slave "$(typeset -f configure_spark); configure_spark"

