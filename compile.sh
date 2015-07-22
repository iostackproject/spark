#!/bin/bash


usage="
Usage:
$(basename "$0") [-h] [-m module names]

Build and package spark project to dist/ and spark-2.6.0-bin-spark-swift.tgz respectively
If -m parameter is not set then all modules will be build
where:
		-h		show this help text
		-m		comma separated list of module names to build

Available module names:
	core
	bagel
	graphx
	mllib
	tools
	network/common
	network/shuffle
	streaming
	sql/catalyst
	sql/core
	sql/hive
	unsafe
	assembly
	external/twitter
	external/flume
	external/flume-sink
	external/mqtt
	external/zeromq
	examples
	repl
	launcher
	external/kafka
	external/kafka-assembly
	swift
"

moduleArgs=""
correctFormat=0

if [ $# -eq 0 ]
then
	while true; do
	    read -p "Do you really wish to compile all modules of Spark (it can take more than one hour)? [y,n] " yn
	    case $yn in
	        [Yy]* ) break;;
	        [Nn]* ) exit;;
	        * ) echo "Please answer yes or no.";;
	    esac
	done
fi

while getopts ':hm:' option; do
	case "$option" in
		h)	echo "$usage"
				exit
				;;
    m)	# Ensure previous build exist
    		if [ -e "dist/" ]
     		then
    			moduleArgs="-pl $OPTARG"
    		fi
    		correctFormat=1
				;;
    :)	printf "missing argument for -%s\n" "$OPTARG" >&2
				echo "$usage" >&2
				exit 1
				;;
   	\?)	printf "illegal option: -%s\n" "$OPTARG" >&2
				echo "$usage" >&2
				exit 1
				;;
  esac
done

if [ $correctFormat -eq 0 ]
then
	echo "$usage" >&2
	exit 1
fi

echo "A executar ./make-distribution.sh --name spark-swift --tgz -Phadoop-2.6 ${moduleArgs}"
./make-distribution.sh --name spark-swift --tgz -Phadoop-2.6 ${moduleArgs}

