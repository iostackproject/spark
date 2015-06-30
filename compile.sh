#!/bin/bash
echo 'Compile and build Spark'

build/mvn -DskipTests clean package | tee make_out.log
#build/mvn -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests clean package | tee make_out.log

if grep -Fq "[ERROR] " make_out.log
then
	echo "Compilation error. See make_out.log for more details"
else
	./make-distribution.sh --name custom-spark --tgz
#	./make-distribution.sh --name custom-spark --tgz -Phadoop-2.6 -Pyarn
	echo 'Completed compilation'
fi



