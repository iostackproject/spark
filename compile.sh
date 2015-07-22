#!/bin/bash

# Usage:
# ./compile
#			make all modules and packaging all project to spark-2.6.0-bin-spark-swift.tgz
# ./compile <any words>
#			make only swift module and packaging all project to spark-2.6.0-bin-spark-swift.tgz
#
#
# To build more modules, add a comma separated list of module names behind the -pl argument
# Add custom parameters to mvn at the end
# Recap:			./make-distribution.sh --name <build-name> --tgz <mvn arguments>
#							<mvn arguments>			-Phadoop-2.6 -Pyarn -pl swift,core,graphx

if [ $# -ne 0 ]
then
	./make-distribution.sh --name spark-swift --tgz -Phadoop-2.6 -pl examples
else
	while true; do
	    read -p "Do you wish to compile all modules of Spark?" yn
	    case $yn in
	        [Yy]* ) make install; break;;
	        [Nn]* ) exit;;
	        * ) echo "Please answer yes or no.";;
	    esac
	done
	./make-distribution.sh --name spark-swift --tgz -Phadoop-2.6
fi
