#!/bin/bash

# Usage:
# ./compile
#			make all modules and packaging all project to spark-2.6.0-bin-spark-swift.tgz
# ./compile <module name>
#			make only the module named and packaging all project to spark-2.6.0-bin-spark-swift.tgz
# ./compile <comma separated list of module names>
#			make all named modules and packaging all project to spark-2.6.0-bin-spark-swift.tgz

if [ $# -ne 0 ]
then
	./make-distribution.sh --name spark-swift --tgz -Phadoop-2.6 -pl $@
else
	while true; do
	    read -p "Do you really wish to compile all modules of Spark (it can take more than one hour)? [y,n] " yn
	    case $yn in
	        [Yy]* ) ./make-distribution.sh --name spark-swift --tgz -Phadoop-2.6; break;;
	        [Nn]* ) exit;;
	        * ) echo "Please answer yes or no.";;
	    esac
	done
	./make-distribution.sh --name spark-swift --tgz -Phadoop-2.6
fi
