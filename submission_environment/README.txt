README
------


Installation:
-------------

In order to run the scripts in this folder you need to compile the following two things first
  1. ZeroMQ in "dependencies/zeromq-2.1.7" (currently storm needs this veresion of zeromq)
  2. ZeroMQ Java-Bindings in "dependencies/jzmq"
Both of them need to be installed into the "dependencies/bin" directory.


On a debian linux system (kraken is such a system) you need to make that the following packages 
are awailable for compliation. Currently they are installed on hector.ifi.uzh.ch (thanks to HP):
  pkg-config
  build-essential
  uuid-dev
  libtool
  autoconf
  openjdk-6-jdk
  
For controlling the distribution of the tasks on processors, the util "taskset" must be 
present. By default this is present on debian / ubuntu like systems. But you may need to 
install it.


More information can be found here: https://github.com/nathanmarz/storm/wiki/Installing-native-dependencies. 


We install the dependencies locally, therefore you need to make sure that the installation 
directory is in your path variable. You can do this by adding the following line to 
you ~/.profile script:



# compile and install zero-mq locally
cd /home/user/lfischer/katts/submission_environment/dependencies/zeromq-2.1.7
./configure --prefix /home/user/lfischer/katts/submission_environment/dependencies/bin
make
make install

# add the installation directory to your PATH environment variable
export PATH="/home/user/lfischer/katts/submission_environment/dependencies/bin:$PATH"
# set the java home directory to your JAVA_HOME environment variable
JAVA_HOME="/usr/lib/jvm/java-6-openjdk"; export JAVA_HOME


# compile and install zero-mq bindings locally
cd /home/user/lfischer/katts/submission_environment/dependencies/jzmq
./autogen.sh --prefix /home/user/lfischer/katts/submission_environment/dependencies/bin
./configure --with-zeromq=/home/user/lfischer/katts/submission_environment/dependencies/bin --prefix /home/user/lfischer/katts/submission_environment/dependencies/bin
make
make install




Usage:
------

./job.sh: This script runs a given job on the cluster.
./experiment.sh: This script builds and runs experiments.


Explanations:
-------------

Job:          A job is a single KATTS query and one cluster setup. A job consists of a query xml, a 
              bootstrap.sh, some data and the KATTS.jar. The bootstrap.sh is used to define the 
              cluster setup. The KATTS.jar contains the storm topology components (Bolts & Spouts).
              The query.xml contains the query in XML form. A sample job can be found in 
              ./samples/sample_job.
              When a job is executed a directory "tmp" and "evaluation" is created inside the 
              job directory.

Experiment:   A experiment is a set of KATTS job. A experiment is used to build and run similar 
              jobs. This can be used to generate for example jobs that differ in the number of
              processors or nodes available to the job. A sample experiment can be found 
              in ./samples/sample_experiment.
              A experiment consists primarly of build.sh and a job template. Where the build.sh
              contains the logic to generate the jobs. The job template is a regular job, where
              the bootstrap.sh and query.xml can contain the variables defined in the build.sh.


Dirs:
-----

katts_jobs:   This folder contains the jobs available for execution. You can also use symbolic
              links to add jobs to this directory. This jobs must be run by ./job.sh. 


experiments:  This directory contains the available experiments. You can also use symbolic
              links to add experiments to this directory. This experiments must be build by
              the ./experiment.sh. 

dependencies: This directory contains the dependencies of the KATTS systems. This are ZeroMQ,
              ZooKeeper, ZeroMQ Java Bindings and Storm.

scripts:      This direectory contains some scripts / executables required to run a job.

samples:      This directory contains some samples. 
