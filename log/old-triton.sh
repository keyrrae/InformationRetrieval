#!/bin/bash

#PBS -q small
#PBS -N WordCountSample
#PBS -l nodes=2:ppn=1
#PBS -o tyang.out
#PBS -e tyang.err
#PBS -l walltime=00:10:00
#PBS -A tyang-ucsb
#PBS -V
#PBS -M tyang@cs.ucsb.edu
#PBS -m abe

# Shell program to execute WordCount using Hadoop.
# To run this program, you need to make sure ~/log/templog1 file exists as the input file.
# no other files need to be created first. All output files are created under ~/log
# Then you can do "qsub log.sh" to execute this file in a queue
# or you can do "qsub -I", and then "sh log.sh" as an interactive mode

# The above instructions set up 1 node and allocates at most 10 mins. 
# the hadoop file system will be namted tyang-ucsb  
# under /state/partition1/hadoop-$USER/data  
# the trace log files are tyang.out and tyang.err



# Set this to location of myHadoop on Triton. That is the system code/libry etc.
# used to run Hadoop/Mapreduce under a supercomputer environment
# Notice the mapreduce Hadoop directory is available in the cluster allocated
#   for exeucting your job (but not necessarily in the triton login node).
export MY_HADOOP_HOME="/opt/hadoop/hadoop-0.20.2/contrib/myHadoop"

# Set this to the location of Hadoop on Triton
export HADOOP_HOME="/opt/hadoop/hadoop-0.20.2"

#### Set this to the directory where Hadoop configs should be generated
# under "/home/tyang-ucsb/wc1/ConfDir"
# that is done automatically based on the above configuration setup.
# Don't change the name of this variable (HADOOP_CONF_DIR) as it is
# required by Hadoop - all config files will be picked up from here
#
# Make sure that this is accessible to all nodes
export HADOOP_CONF_DIR="/home/tyang-ucsb/log/ConfDir"


#### Set up the configuration
# Make sure number of nodes is the same as what you have requested from PBS
# usage: $MY_HADOOP_HOME/bin/configure.sh -h
echo "Set up the configurations for myHadoop please..."
$MY_HADOOP_HOME/bin/configure.sh -n 2 -c $HADOOP_CONF_DIR 

# -p -d "/home/tyang-ucsb/mapreduce/result"

#Format the Hadoop file system initially. It is possible that you donot need to call when executing multiple times.
# But after a while, this file system will be removed anyway, thus we may have to recreate again.
echo "Format HDFS"
#$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR namenode -format
echo

#### Start the Hadoop cluster
echo "Start all Hadoop daemons"
$HADOOP_HOME/bin/start-all.sh
#$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave

#### Clear Result folder
#rm /home/tyang-ucsb/result/*

#### Run your jobs here


#echo "Creating input directory in HDFS "
#The input data is created under the hadoop system with name input/a. 
# The data is fetched from the local system ~/wc1/a.
#$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -mkdir in1 
echo "Copy data to HDFS .. "

$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyFromLocal ~/log/templog1 input/a

#Now we really start to run this job. The intput and output directories are under the hadoop file system.
echo "Run log analysis job .."
time $HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar loganalyzer.jar LogAnalyzer input output

echo "Check output files after PC.. but i remove the old output data first"
rm -r ~/log/output
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal output ~/log/output
cp -r  $HADOOP_HOME/logs ~/log

#### Stop the Hadoop cluster
echo "Stop all Hadoop daemons"
$HADOOP_HOME/bin/stop-all.sh
echo

#### Clean up the working directories after job completion
## that may remove the filesystem created.
echo "Clean up .."
$MY_HADOOP_HOME/bin/cleanup.sh -n 2
echo
