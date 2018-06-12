#!/bin/ksh
set -x

#####################################################################################
# Script: credit_app_fact_build.sh
#
# Description: Job flow to build the EAGLE credit app table
#
# Modification History:
#
# 2018-05-15 Paritosh Rohilla             Initial Revision
#####################################################################################

# Setting up these variable to enable logging of commands executed in this shell script

export homeDir=$(pwd)/..
mkdir -p LOGS/test_credit_app_fact_build
export logDir=$homeDir/LOGS
export logFile=$logDir/credit_app_fact_build

# Now redirect shell output to $logFile
# Save stdout and stderr to file descriptors 3 and 4, then redirect them to log file 
exec 3>&1 4>&2 >$logFile.$$.log 2>&1

#####################################################################################
# Usage
#####################################################################################
usage() {
print "Usage:  credit_app_fact_build.sh <environ>; e.g. prd"
}

# Check passed arguments
# Test if we have two arguments on the command line
if [[ $# != 1 ]]
then
    usage
    exit
else
    argEnv=$1
fi


#####################################################################################
# Set variables
#####################################################################################

export scriptDir=$homeDir/scripts
export restartFile=$logDir/credit_app_fact_restart
export env=$argEnv
export rundate=$(date '+%Y%m%d')

# Set beeline server 
if [[ $argEnv == dev ]]; then
  BEELINE_HOST="tbldaxa01adv-hdp.tdc.vzwcorp.com"
else
  BEELINE_HOST="tbldaxa02apd-hdp.tdc.vzwcorp.com"
fi

# Create restart file if it does not exist and set it to start at STEP 1
test -e $restartFile || print 1 > $restartFile
restartStep=`cat $restartFile`

# Use case to go to the proper step
while true
do
case $restartStep in

1)
#####################################################################################
# Step 1 - Build fact table 
#####################################################################################

export hqlScript=$scriptDir/credit_app_fact.hql
print "Executing step $restartStep - Script $hqlScript"

beeline -u "jdbc:hive2://${BEELINE_HOST}:10000/default;principal=hive/_HOST@VBD.KRB.VZWCORP.COM" \
-hivevar ENV=$env \
-f $hqlScript

retCode=$?
if [[ $retCode -ne 0 ]]; then
  print "Error in $hqlScript"
  exit $retCode
else
  print "Success executing $hqlScript"
fi

# Go to next step
restartStep=2
print $restartStep > $restartFile
;;

2)
#########################################################################################
# Step 2
#########################################################################################

# Come out of the while loop as this is the last step
break
;;

# Unspecified step
*)
echo "Wrong restart step. Pleae check your restart file!!!!"
exit 1

;;

esac
done

#####################################################################################
# All went well. Delete restart file and exit gracefully
#####################################################################################
print "Success executing all steps"
rm $restartFile

# Restore stdout and stderr
exec 1>&3 2>&4

exit 0
