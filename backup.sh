#!/bin/bash

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
# Script that creates full and incremental backups of all tables in a particular database.
# Full backups are made on tuesday and on friday.
# Backups are stored in files /data/chstore-backup/<database_name>/<backup_date>_<backup_type>/<table_name>.zip
# where backup_date is written in YYYYMMDD format and backup_type means full backup or incremental one.
#
# The script checks if any backup of a certain table  has already been made. 
# If false (backup table does not exist) then full backup is made no matter what.
# If any backup file was found then at least one full backup is made and the script will create an incremental one.
#
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------------------
# Global variables
#-----------------------------------------------------------------------------------------------------------------------
# CH creds.
user=<user_name>
pass=<password>

# Prefix to execute insude container.
docker='docker exec <container_name>'

# Get the list of all tables from selected database.
database="$1"
tables=$($docker clickhouse-client -u "$user" --password "$pass" \
       -q "SELECT name
	   FROM system.tables
	   WHERE database = '$database'
	   FORMAT TabSeparated;")

# Get date format for backup directory name and get the day of the week.
date=$(date +%Y%m%d)
week_day=$(date +%u)


#-----------------------------------------------------------------------------------------------------------------------
# Functions definition
#-----------------------------------------------------------------------------------------------------------------------
do_full_backup() {
	$docker clickhouse-client -u "$user" --password "$pass" \
		-q "BACKUP TABLE "$1"."$2" TO Disk('backup_disk', '"$1"/"$date"_full/"$2".zip');"
}

do_incremental_backup() {
	$docker clickhouse-client -u "$user" --password "$pass" \
		-q "BACKUP TABLE "$1"."$2" TO Disk('backup_disk', '"$1"/"$date"_incr/"$2".zip')
		    SETTINGS base_backup=${3//\\/};"
}

# Base backup is needed for incremental one.
get_base_backup() {
	$docker clickhouse-client -u "$user" --password "$pass" \
		-q "SELECT name
		    FROM system.backups
		    WHERE status=1
		    AND name LIKE '%\'"$1"/%/"$2"%'
		    AND end_time > now() - INTERVAL 10 DAY
		    ORDER BY start_time DESC
		    LIMIT 1;"
}

# Get the number of backup files for a certain table.
num_backup_files() {
	$docker find /data/chstore-backup/"$1" -name "$2.zip" | wc -l 
}


#-----------------------------------------------------------------------------------------------------------------------
# Start script
#-----------------------------------------------------------------------------------------------------------------------

main() {

	if [[ "$week_day" -eq 2 ]] || [[ "$week_day" -eq 5 ]] ; then	# Check day of the week number
		
		for table in $tables
		do	
			do_full_backup "$database" "$table"
		done
	else
		for table in $tables
		do	
			if [[ $(num_backup_files "$database" "$table") -ne 0 ]] ; then	# Check if there is any backup file for every table.
											# If the file exists than create incremental backup.
				base_backup=$(get_base_backup "$database" "$table")
				do_incremental_backup "$database" "$table" "$base_backup"
			else
				do_full_backup "$database" "$table"
			fi	
		done
	fi
}

main
