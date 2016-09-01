#!/bin/sh
#-----------------------------------------------------
# NAME:    Export Data from bigquery 
# PURPOSE: Export Data from bigquery
# $Header$ Export Data from bigquery
# $Author$ Rajnish Kumar
#Scope: Export Data from bigquery table through query or complete table. 

#-----------------------------------------------------
##
SCRIPT_NAME=`basename $0`
debug=0
verbose=0
DOWNLOAD_LOCAL=0
SFTP_TRANSFER=0
BigQueryDelete=0
HILIGHT="\033[1;37m"
NRML="\033[0m"
#-----------------------------------------------------
# Checking parameters
#-----------------------------------------------------
while [ $# -gt 0 ]
do
  case "$1" in
     -project_id) PROJECT_ID=$2;shift;;
	 -dataset) DATASET=$2;shift;;
	 -query) QUERY=$2; shift;;
     -table_name) TABLE_NAME=$2; shift;;
	 -bucket_name) BUCKET_NAME=$2; shift;;
	 -local_path) Destination_Path=$2; shift;;
     -ftp_user) SFTP_User=$2;shift;;
	 -ftp_pass) SFTP_PASS=$2;shift;;
	 -ftp_server) SFTP_IP=$2; shift;;
	 -ftp_port) SFTP_PORT=$2; shift;;
     -export_file_name) EXPORT_FILE_NAME=$2; shift;;	 
     -download_local) DOWNLOAD_LOCAL=1;;
     -sftp_transfer) SFTP_TRANSFER=1;;
     -debug) debug=1;;
     -verbose) verbose=1;;
      -h|-H|-help|-HELP)  echo -e \
               "$HILIGHT NAME$NRML\n" \
               "$SCRIPT_NAME - Export Data From Google BigQuery \n\n$HILIGHT" \
               "SYNOPSYS$NRML\n" \
               "DESCRIPTION$NRML\n" \
               " or $SCRIPT_NAME -h to see ALL detailed options help\n$HILIGHT" \
			   "Note : mandatory fields are mark with * \n"\
               "OPTIONS$NRML\n" \
			   "-project_id <Name of the Google Project_id,*) >\n" \
			   "-dataset <Name of the dataset/Databases,*) >\n" \
			   "-query <Data export Query) >\n" \
			   "-table_name <Table name for data export,(either pass query or tablename)* not both,) >\n" \
			   "-bucket_name <Name of the Google Storage Name,*) >\n" \
			   "-download_local <If you want to download export data into local file system ) >\n" \
			   "-local_path <Location on local system where we download export file,* if you choose -download_local,Default directory is tmp ) >\n" \
			   "-sftp_transfer <if you wanna transfer export file to sftp location ) >\n" \
			   "-ftp_user <sftp user name,* when choose -sftp_transfer  ) >\n" \
			   "-ftp_pass <sftp user password,* when choose -sftp_transfer  ) >\n" \
			   "-ftp_server <sftp server ip address,* when choose -sftp_transfer  ) >\n" \
			   "-ftp_port <sftp port number,* when choose -sftp_transfer  ) >\n" \
			   "-export_file_name <file name to save export data,) >\n" 
			  exit 0;;

       *) break;;	# terminate while loop
  esac
  shift
done


##
if [ $debug -eq 1 ]; then
 set -x
fi

fnLog(){
DIR_LOG=/tmp
TIME=`date +"%d%m%Y_%H_%M_%S"`
DATE=`date +"%d%m%Y"`
LOGFILE='BigQueryDataExportData'_${DATE}  
  if [ $verbose -eq 1 ]; then
     echo $TIME"-> "$* | tee -a  $DIR_LOG/$LOGFILE
  else
     echo $TIME"-> "$* >>  $DIR_LOG/$LOGFILE
  fi
}



setEnvironment() {

if [ -z ${PROJECT_ID} ]; then
     fnLog PROJECT_ID is NULL. Please use -project_id  
	 echo "PROJECT_ID is NULL. Please use -project_id "
     exit 1
     
fi 

if [ -z ${DATASET} ]; then
     fnLog DATASET is NULL. Please use -dataset  
	 echo "DATASET is NULL. Please use -dataset "
     exit 1
     
fi 


if [ -z ${BUCKET_NAME} ]; then
     fnLog BUCKET_NAME is NULL. Please use -bucket_name  
	 echo "BUCKET_NAME is NULL. Please use -bucket_name "
     exit 1
     
fi 

if [ -z ${Destination_Path} ]; then
     fnLog Destination_Path is NULL. Going to use Default local_path=/tmp/ or use -local_path  
	 echo "Destination_Path is NULL. Going to use Default local_path=/tmp/ or use -local_path "
     Destination_Path=/tmp/
     
fi 



FileTIME=`date +"%d%m%Y_%H_%M_%S"`
TimeStampLog=`date +%s`
EXPORT_DATA='EXPORT_DATA'
BigQueryBQ=/usr/local/bin/bq
GSUTIL=/usr/local/bin/gsutil
TAR=/bin/tar
MORE=/bin/more
}


export_data() {

if [  -z "$QUERY"  ] && [ -z ${TABLE_NAME} ] ; then
     fnLog QUERY and TABLE_NAME Both are NULL. please provide either table name or query for data export, use -query or -table_name 
	 echo "QUERY and TABLE_NAME Both are NULL. please provide either table name or query for data export, use -query or -table_name"
	 fnLog - ${SCRIPT_NAME} ends with ERROR -
     exit 1
else
		if [ ! -z "$QUERY" ] && [ ! -z ${TABLE_NAME} ] ; then
		fnLog QUERY and TABLE_NAME Both are NOT NULL. please use  either table name or query for data export, use -query or -table_name 
		echo "QUERY and TABLE_NAME Both are NoT NULL. please use  either table name or query for data export, use -query or -table_name"
		fnLog - ${SCRIPT_NAME} ends with ERROR -
		exit 1
		else 
			if [ -z ${TABLE_NAME} ]; then
			fnLog "TABLE_NAME is NULL. Going to  Export data using query :" "$QUERY"  
			echo  "TABLE_NAME is NULL. Going to  Export data using query : " "$QUERY"
			export_Query_result "$QUERY"
				else
				fnLog TABLE_NAME is NOT NULL. Going to  Export data using TABLE_NAME : ${TABLE_NAME}  
				echo "TABLE_NAME is not NULL. Going to  Export data using TABLE_NAME : ${TABLE_NAME} "
				table_export ${TABLE_NAME}
			fi
	    fi 
 
fi

}

export_Query_result() {
exportquery=$1
destination_table=Export_Table_${TimeStampLog}
export_job_id_file=/tmp/export_job_id_file_${TimeStampLog}.txt
export_status=/tmp/export_status_${TimeStampLog}.txt
fnLog "export Table Name" $destination_table 
echo "export Table Name" "destination_table" 
${BigQueryBQ} query  --batch --nosync  --allow_large_results --destination_table=${DATASET}.${destination_table} "${exportquery}; "  >  ${export_job_id_file} 

GetJobID=`sed -n 's/.*query//p' ${export_job_id_file}`

wait_export_status=0

while [ $wait_export_status -eq 0 ]
do
${BigQueryBQ} show -j ${GetJobID} > ${export_status}
wait_export_status=`cat ${export_status} | grep 'SUCCESS' | wc -l`
fnLog "wait_export_status:" $wait_export_status
echo  "wait_export_status:" $wait_export_status
if [ $wait_export_status == 0 ]
then
	fnLog export is not completed ,waiting for it.
	echo " export is not completed ,waiting for it."
    sleep 5
else
     fnLog Going to  Export data using TABLE_NAME : ${TABLE_NAME}  
	 echo "Going to  Export data using TABLE_NAME : ${TABLE_NAME} "
	 BigQueryDelete=1
	 table_export ${destination_table} 
 	
fi   


done


}


table_export()  {
tablename=$1
Export_Table_name=${tablename}_${TimeStampLog}
fnLog export_table_name : ${tablename}
echo "Export_table_name : ${tablename}"
${BigQueryBQ} extract --destination_format=CSV ${DATASET}.${tablename} gs://${BUCKET_NAME}/${EXPORT_DATA}/${Export_Table_name}/${tablename}*

fnLog  -- Data export successfull : Location is ${BUCKET_NAME}/${EXPORT_DATA}/${Export_Table_name} ---
echo " -- Data export successfull : Location is ${BUCKET_NAME}/${EXPORT_DATA}/${Export_Table_name} ---"

if [ $BigQueryDelete -eq 1 ]; then
fnLog "going to delete bigquery table : ${Export_Table_name} "
echo "going to delete bigquery export table:" ${Export_Table_name}
${BigQueryBQ} rm -f -t ${DATASET}.${tablename}
else
fnLog "TABLE NOT deleted bigquery table : ${Export_Table_name} "
echo "TABLE NOT deleted bigquery export table:" ${Export_Table_name}
fi

}

download_local() {
if [ $DOWNLOAD_LOCAL -eq 1 ]; then
fnLog -- Going to Download export file to Local File system ---
echo "-- Going to Download export file to Local File system ---"
$GSUTIL cp -r gs://${BUCKET_NAME}/${EXPORT_DATA}/${Export_Table_name} ${Destination_Path}
Merge_Files
compress_folder
TransferFile_SFTP
fi

}

Merge_Files() {

cd ${Destination_Path}/${Export_Table_name}
Tmp_AllDownloadedFiles=/tmp/tmp_${Export_Table_name}.txt
AllDownloadedFiles=/tmp/${Export_Table_name}.txt
#echo "tmp filename :" $Tmp_AllDownloadedFiles
#echo "filename :" $AllDownloadedFiles

ls -1 ${Destination_Path}/${Export_Table_name} | tr '\n' '\0' | xargs -0 -n 1 basename  > ${Tmp_AllDownloadedFiles}

more +2 ${Tmp_AllDownloadedFiles} > ${AllDownloadedFiles}


FileCount=`cat ${Tmp_AllDownloadedFiles} |wc -l` 

if [ $FileCount -gt 1 ]; then
while IFS='' read -r line || [[ -n "$line" ]]; do
cd ${Destination_Path}/${Export_Table_name}
#echo "filename:" $line
processes_file=$line.csv
#echo "processfile:" $processes_file
tail -n +2 ${line} > ${processes_file}
rm -f $line
done < ${AllDownloadedFiles}

else
#fnLog file count  is less than 2 , so no file merging 
echo "file count is less than 2, so no file merging "
fi

if [ -z ${EXPORT_FILE_NAME} ]; then
     fnLog EXPORT_FILE_NAME is NULL. Going to generate it or use -export_file_name  
	 echo "EXPORT_FILE_NAME is NULL. Going to generate it or use -export_file_name "
	 merge_file_name=Complete_${Export_Table_name}.csv
	 echo ${merge_file_name}
else	 
     echo "data export file name is :" ${EXPORT_FILE_NAME}
	 merge_file_name=${EXPORT_FILE_NAME}.csv
	 echo ${merge_file_name}
     
fi

if [ $FileCount -gt 1 ]; then

cd ${Destination_Path}/${Export_Table_name}
#fnLog "merging all files into single file"
echo "merging all files into single file"

first_file=`head -1 ${Tmp_AllDownloadedFiles}`
cat ${first_file} >  Complete_Export_${Export_Table_name}
cat *.csv >> Complete_Export_${Export_Table_name}
mv Complete_Export_${Export_Table_name} ${merge_file_name}
find . ! -name ${merge_file_name} -type f -exec rm -f {} +
else
fnLog "Merging Not Required,renaming export file name"
echo "Merging Not Required, re naming  export file name"
first_file=`head -1 ${Tmp_AllDownloadedFiles}`
echo ${merge_file_name}
mv ${first_file} ${merge_file_name}
fi

}

compress_folder() {
rename=0
if [ -z ${EXPORT_FILE_NAME} ]; then
     fnLog EXPORT_FILE_NAME is NULL. Going to generate it or use -export_file_name  
	 echo "EXPORT_FILE_NAME is NULL. Going to generate it or use -export_file_name "
	 rename=0
else	 
     echo "data export file name is :" ${EXPORT_FILE_NAME}
	 Export_Table_name1=${EXPORT_FILE_NAME}
	 rename=1
     
fi



if [ $rename -eq 0 ]; then
cd ${Destination_Path}
fnLog compress folder ${Destination_Path}/${Export_Table_name}
echo "compress folder:" ${Destination_Path}/${Export_Table_name}
$TAR -zcvf ${Export_Table_name}.tar.gz  ${Export_Table_name}
rm -rf ${Destination_Path}/${Export_Table_name}
else
cd ${Destination_Path}
mv ${Export_Table_name} ${Export_Table_name1}
Export_Table_name=${Export_Table_name1}
fnLog compress folder ${Destination_Path}/${Export_Table_name}
echo "compress folder:" ${Destination_Path}/${Export_Table_name}
$TAR -zcvf ${Export_Table_name}.tar.gz  ${Export_Table_name}
rm -rf ${Destination_Path}/${Export_Table_name}

fi
}

TransferFile_SFTP () {

if [ $SFTP_TRANSFER -eq 1 ]; then

if [ -z ${SFTP_User} ]; then
     fnLog SFTP_User is NULL.  use -ftp_user  
	 echo "SFTP_User is NULL.  use -ftp_user "
     exit 1
     
fi 

if [ -z ${SFTP_PASS} ]; then
     fnLog SFTP_PASS is NULL.  use -ftp_pass  
	 echo "SFTP_PASS is NULL.  use -ftp_pass "
     exit 1
     
fi 

if [ -z ${SFTP_IP} ]; then
     fnLog SFTP_IP is NULL.  use -ftp_server  
	 echo "SFTP_IP is NULL.  use -ftp_server "
     exit 1
     
fi

if [ -z ${SFTP_PORT} ]; then
     fnLog SFTP_PORT is NULL.  use -ftp_port  
	 echo "SFTP_PORT is NULL.  use -ftp_port"
     exit 1
     
fi


cd ${Destination_Path}
#echo "sftp username :" $SFTP_User
#echo "sftp password :" $SFTP_PASS
#echo "sftp server ip :" $SFTP_IP
#echo "file path :" $Destination_Path
SSHPASS=${SFTP_PASS} sshpass -e  sftp -oPort=${SFTP_PORT} ${SFTP_User}@${SFTP_IP} << $!#
put -r ${Destination_Path}/${Export_Table_name}.tar.gz   
$!#
else
fnLog "sftp_transfer is null , not going to trnsfer it on sftp location"
echo "sftp_transfer is null , not going to trnsfer it on sftp location"
fi

}




#---------------------------------------------------------------
# main: launches all the commands
#---------------------------------------------------------------
main(){
fnLog Creating LOG $DIR_LOG/$LOGFILE
fnLog --- ${SCRIPT_NAME} starts ---
setEnvironment
export_data
download_local
fnLog --- ${SCRIPT_NAME} ends ---
exit 0
}

main



