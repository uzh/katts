README
------

In order to run the scripts in this folder you need to compile the following two things first
  1. ZeroMQ in "dependencies/zeromq-2.1.7" (currently storm needs this veresion of zeromq)
  2. ZeroMQ Java-Bindings in "dependencies/jzmq"
Both of them need to be installed into the "dependencies/bin" directory.


On a debian linux system (kraken is such a system) you need to make that the following packages are awailable for compliation. Currently they are installed on hector.ifi.uzh.ch (thanks to HP):
  pkg-config
  build-essential
  uuid-dev
  libtool
  autoconf
  openjdk-6-jdk


More information can be found here: https://github.com/nathanmarz/storm/wiki/Installing-native-dependencies. 


We install the dependencies locally, therefore you need to make sure that the installation directory is in your path variable. You can do this by adding the following line to you ~/.profile script:



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