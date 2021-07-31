#!/bin/bash

download__hadoop () {
	## Go to Home Folder
	cd ~

	## Download the hadoop tar
	wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz

	## Extract hadoop tar 
	tar -xzf hadoop-2.7.7.tar.gz

	## Export environmental variables.
	echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
	echo 'export HADOOP_INSTALL=/home/user/hadoop-2.7.7' >> ~/.bashrc
	echo 'export PATH=$PATH:$HADOOP_INSTALL/bin' >> ~/.bashrc
	echo 'export PATH=$PATH:$HADOOP_INSTALL/sbin' >> ~/.bashrc
	echo 'export HADOOP_HOME=$HADOOP_INSTALL' >> ~/.bashrc
	echo 'export HADOOP_COMMON_HOME=$HADOOP_INSTALL' >> ~/.bashrc
	echo 'export HADOOP_HDFS_HOME=$HADOOP_INSTALL' >> ~/.bashrc
	echo 'export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop' >> ~/.bashrc

	source ~/.bashrc

}

configure_hadoop () {	
## Edit core-site.xml to set hdfs default path to hdfs://master:9000
		CORE_SITE_CONTENT="\t<property>\n\t\t<name>fs.default.name</name>\n\t\t<value>hdfs://master:9000</value>\n\t</property>"
		INPUT_CORE_SITE_CONTENT=$(echo $CORE_SITE_CONTENT | sed 's/\//\\\//g')
		sed -i "/<\/configuration>/ s/.*/${INPUT_CORE_SITE_CONTENT}\n&/" /home/user/hadoop-2.7.7/etc/hadoop/core-site.xml


## Edit hdfs-site.xml to set hadoop file system parameters
		HDFS_SITE_CONTENT="\t<property>\n\t\t<name>dfs.replication</name>\n\t\t<value>2</value>\n\t\t<description>Default block replication.</description>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.namenode.name.dir</name>\n\t\t<value>/home/user/hdfsname</value>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.datanode.data.dir</name>\n\t\t<value>/home/user/hdfsdata</value>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.blocksize</name>\n\t\t<value>64m</value>\n\t\t<description>Block size</description>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.webhdfs.enabled</name>\n\t\t<value>true</value>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.support.append</name>\n\t\t<value>true</value>\n\t</property>"
		INPUT_HDFS_SITE_CONTENT=$(echo $HDFS_SITE_CONTENT | sed 's/\//\\\//g')
		sed -i "/<\/configuration>/ s/.*/${INPUT_HDFS_SITE_CONTENT}\n&/" /home/user/hadoop-2.7.7/etc/hadoop/hdfs-site.xml

## Set the two datanodes for the distributed filesystem
		echo "master" > /home/user/hadoop-2.7.7/etc/hadoop/slaves
		echo "slave" >> /home/user/hadoop-2.7.7/etc/hadoop/slaves

## Export JAVA_HOME variable for hadoop
		sed -i '/export JAVA\_HOME/c\export JAVA\_HOME=\/usr\/lib\/jvm\/java-8-openjdk-amd64' /home/user/hadoop-2.7.7/etc/hadoop/hadoop-env.sh
}

echo "STARTING DOWNLOAD ON MASTER"
download__hadoop

echo "STARTING DOWNLOAD ON SLAVE"
ssh user@slave "$(typeset -f download__hadoop); download__hadoop"

echo "STARTING HADOOP CONFIGURE ON MASTER"
source ~/.bashrc; configure_hadoop

echo "STARTING HADOOP CONFIGURE ON SLAVE"
ssh user@slave "$(typeset -f configure_hadoop); configure_hadoop"

