#!/bin/bash
#PBS -N katts
#PBS -l nodes=12:ppn=23
#PBS -l walltime=604800,cput=2400000,mem=55000mb
#PBS -j oe
#PBS -m b
#PBS -m e
#PBS -m a
#PBS -V

hostname=`hostname`
working_dir=`mktemp -d --tmpdir=/var/tmp`
cd $working_dir

# copy dependencies
cp ~/katts/ $working_dir/

# setup ZooKeeper

# install ZeroMQ

# install ZeroMQ Java Bindings

# Start Storm Supervisor 



# Where to run the Nimbus?

# On which node we deploy the Nimbus and how to execute the toplology?



# remove temporary directory
cd ~
rm -rdf $working_dir