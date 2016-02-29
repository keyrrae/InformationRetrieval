#!/bin/bash
#SBATCH --job-name="loganalysis"
#SBATCH --output="loganalysis.%j.%N.out"
#SBATCH --partition=compute
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=24
#SBATCH -t 00:15:00
export HADOOP_CONF_DIR=/home/$USER/cometcluster
export WORKDIR=`pwd`
module load hadoop/1.2.1
myhadoop-configure.sh
start-all.sh
hadoop dfs -mkdir input

echo "Copy data to HDFS .. "
#hadoop dfs -copyFromLocal $WORKDIR/test.txt input/
hadoop dfs -copyFromLocal $WORKDIR/trafficdata/*.* input

#Now we really start to run this job. The intput and output directories are under the hadoop file system.
#hadoop jar $WORKDIR/wordcount.jar  wordcount input/ output/
echo "Run log analysis job .."
time hadoop jar loganalyzer.jar LogAnalyzer input output


echo "Check output files from HDFS.. but i remove the old output data first"
rm -rf log-out >/dev/null || true
mkdir -p log-out

hadoop dfs -copyToLocal output log-out
hadoop dfs -copyToLocal result.txt result.txt

stop-all.sh
myhadoop-cleanup.sh
