#!/bin/bash

#This script is NOT very portable. It was created by a Netezza Engineer specifically for one environment. You may
#need to create certain directories, environment variables, and install other utilities to get this to work. 
#It's essentially black magic that we dare not touch because it has worked for us without changes for 3 years.

set -o vi

export PATH=$PATH:/nz/support/contrib/bin:~/freetds-0.82/src/apps
export S=/export/home/nz/management_scripts/

cd $S

DB_NAME=$1
TABLE_NAME=$2

DB_NAME_UPPER=`echo $DB_NAME|tr [:lower:] [:upper:]`

TABLE_NAME_UPPER=`echo $TABLE_NAME|tr [:lower:] [:upper:]`

rm -f $L/${DB_NAME}/${TABLE_NAME_UPPER}.${DB_NAME_UPPER}.nzlog
rm -f $L/${DB_NAME}/${TABLE_NAME_UPPER}.${DB_NAME_UPPER}.nzbad
echo STARTED  at `date`
for i in `echo ${TABLE_NAME}`
do
   echo starting $i at `date`
   nzsql -d ${DB_NAME} -c "truncate table $i;"
if [ "$?" -ne "0" ]; then
  echo "Problem with nzsql command"
  exit 1
fi
if [ ! -d /nz/log/${DB_NAME} ]; then
  mkdir /nz/log/${DB_NAME}
fi
   nzconvert -f ISO-8859-1 -df /mnt/bcp/${DB_NAME}/${i}.txt | dos2unix | tr -d '\000' | nzload -db ${DB_NAME} -t $i -delim '|' -fileBufSize 20 -fillRecord -crInString -encoding internal -maxErrors 1 -outputdir /nz/log/${DB_NAME} -truncString -ctrlChars -escapechar '\'
if [ "$?" -ne "0" ]; then
  echo "Problem with the nzconvert"
  exit 1
fi

done
echo FINISHED at `date`