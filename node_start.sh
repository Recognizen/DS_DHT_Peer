#!/bin/bash

java -cp $AKKA_CLASSPATH:.:/home/recognition/Desktop/CourseWork/DS1/DS_Project/build/classes/ds_project/node NodeApp first &
#cd config_files/
#for f in *; do
#    java -cp $AKKA_CLASSPATH:.:/home/recognition/Desktop/CourseWork/DS1/DS_Project/build/classes/ NodeApp "${f%.*}"  127.0.0.1 10001 &
#done
