#!/bin/bash
#SBATCH --job-name="compile"
#SBATCH --output="compile.%j.%N.out"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 00:05:00

#mkdir -p target
#javac -d target src/*.java
#jar -cvf wordcount.jar  -C target/ .
make
