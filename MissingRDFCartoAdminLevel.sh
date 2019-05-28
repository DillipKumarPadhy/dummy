#!/bin/bash
echo "Please Enter current quarter name "
read -r CURRENT_QUARTER
echo "Please Enter previous quarter name "
read -r PREVIOUS_QUARTER
if [[ "$CURRENT_QUARTER" == "" ]]; then
    echo "CURRENT_QUARTER name is not entered ..."
    exit 1
elif [[ "$PREVIOUS_QUARTER" == "" ]]; then
    echo "PREVIOUS_QUARTER name is not entered ..."
    exit 1
fi


echo "Creating log file for Missing RDF Cartos Admin Level "
ORACLE_HOME=/usr/lib/oracle/12.1/client64
export ORACLE_HOME
DataDirectory='/home/dpadhy/gabq418/MissingRDFCartoAdminLevel/tmp/data'

declare -a regions=(RDF_IND_ RDF_ANT_ DF_AUNZ_ RDF_APAC_ RDF_EEU_ RDF_MEA_ RDF_NA_ RDF_SA_ RDF_TWN_ RDF_WEU_) 
DBUSERPASSWORD='password'
DB='EP1DEV'

for i in "${regions[@]}"
do
 DBUSER=$i$CURRENT_QUARTER
 CURRENTDBUSER=$i$CURRENT_QUARTER
 PREVIOUSDBUSER=$i$PREVIOUS_QUARTER
if [ "$i" != "RDF_IND_" ]; then
 appendcmd="append"
else
 appendcmd=""
fi

var=`$ORACLE_HOME/bin/sqlplus -S ${DBUSER}/${DBUSERPASSWORD}@${DB} << EOD

spool ${DataDirectory}/MissingRDFCarto_$CURRENT_QUARTER.csv ${appendcmd}
set lines 32767
set pages 1500
set feedback off
set trimspool on
set heading on
set colsep ,
set newpage none
set underline off

select  A.CARTO_ID ,A.ADMIN_PLACE_ID ,A.ADMIN_LEVEL  as PrevADMIN_LEVEL,b.ADMIN_LEVEL as CurrentADMIN_LEVEL
from (
(select distinct CA.CARTO_ID ,TS.ADMIN_PLACE_ID ,TA.ADMIN_LEVEL
from ${PREVIOUSDBUSER}.RDF_carto CA 
JOIN ${PREVIOUSDBUSER}.TEST_SHAPE TS ON CA.CARTO_ID=TS.CARTO_ID 
JOIN ${PREVIOUSDBUSER}.TEST_AREA TA ON TS.ADMIN_PLACE_ID = TA.ADMIN_PLACE_ID AND TA.ADMIN_LEVEL in (2,4) 
) a
 join 
(select  distinct CA.CARTO_ID ,TS.ADMIN_PLACE_ID ,TA.ADMIN_LEVEL 
from ${CURRENTDBUSER}.RDF_carto CA 
JOIN ${CURRENTDBUSER}.TEST_SHAPE TS ON CA.CARTO_ID=TS.CARTO_ID 
JOIN ${CURRENTDBUSER}.TEST_AREA TA ON TS.ADMIN_PLACE_ID = TA.ADMIN_PLACE_ID AND TA.ADMIN_LEVEL in (2,4) 
) b
on a.CARTO_ID=b.CARTO_ID and a.ADMIN_PLACE_ID=b.ADMIN_PLACE_ID 
)
where a.ADMIN_LEVEL <> b.ADMIN_LEVEL ;

spool off

exit;
EOD`
 
done

echo "log file created sucessfully"



