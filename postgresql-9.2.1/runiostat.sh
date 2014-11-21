#!/bin/bash
# How to call this script:
# ./runiostat1 <executable> <param1> <param2>
# In fact you are just adding the script name before the normal execution of your script.

CPU_IOSTATOUT=`mktemp __iostat_out.XXXXXXXXXX`
DISK_IOSTATOUT=`mktemp __iostat_disk_out.XXXXXXXXXX`
TOPOUT=`mktemp __top_out.XXXXXXXXXX`
DEVICE=/dev/sdb
CPU_INTERVAL=1 #Change this parameter if you want to measure CPU activity less often than every 1 second
DISK_INTERVAL=1 #Change this parameter if you want to measure DISK activity less often than every 1 second
# ----------------------------------------------------------------------

#iostat -t -d -m ${DEVICE} > ${IOSTATOUT}

#Start the CPU monitoring tool from iostat
iostat -t -c ${CPU_INTERVAL} >> ${CPU_IOSTATOUT}&
#Keep the pid of the CPU monitoring process in order to kill it in the end
CPU_IOSTATPID=$! 

#Start the disk monitoring tool from iostat
iostat -t -d ${DISK_INTERVAL} -x -k ${DEVICE}>> ${DISK_IOSTATOUT}&
#Keep the pid of the disk monitoring process in order to kill it in the end
DISK_IOSTATPID=$!

#Run the application (in the background) and continue with the script
$@ &
APP_PID=$!
#echo ${APP_PID}
#ps -ef | grep ${APP_PID}

#Start the CPU monitoring tool from top
top -b -p ${APP_PID} -d ${CPU_INTERVAL} >> ${TOPOUT}&
#Keep the pid of the top process in order to kill it in the end
TOP_PID=$!

#Wait for the main application to finish (started in line 27)
wait ${APP_PID}
#Kill all three monitoring processes
kill ${CPU_IOSTATPID}
kill ${DISK_IOSTATPID}
kill ${TOP_PID}

###iostat -t -d -m ${DEVICE} >> ${DISK_IOSTATOUT}
####################################
#Printing out the monitoring
####################################
sleep 1
echo Printing results
echo
############
echo CPU from iostat
cat ${CPU_IOSTATOUT} | head -n 4 | tail -n 1
cat ${CPU_IOSTATOUT} | grep "avg-cpu" -A 1 | grep -v "avg-cpu" | awk '{if ($1!="--") print $0}' 
#Instead of deleting the temp file and printing the results you can save the files for further editing.
rm ${CPU_IOSTATOUT} 
echo 
echo CPU from top
cat ${TOPOUT} | head -n 7 | tail -n 1
cat ${TOPOUT} | grep ${APP_PID}
#Instead of deleting the temp file and printing the results you can save the files for further editing.
rm ${TOPOUT}
echo 
echo IO from iostat
cat ${DISK_IOSTATOUT} | head -n 4 | tail -n 1
cat ${DISK_IOSTATOUT} | grep "Device" -A 1 | grep -v "Device" | awk '{if ($1!="--") print $0}' 
cat ${DISK_IOSTATOUT} | grep "Device" -A 1 | grep -v "Device" | awk 'BEGIN{s1=0;s2=0;s3=0;n=0}{n+=1;s1+=$4;s2+=$6;s3+=$9; }END{print s1,s2,s3}'
#Instead of deleting the temp file and printing the results you can save the files for further editing.
rm ${DISK_IOSTATOUT}