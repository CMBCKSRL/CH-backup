#!/bin/bash

#------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------
# Script that creates full and incremental backups of all tables in a particular database.
# Full backups are made on the first day of every month.
# Backups are stored in files /data/chstore-backup/<cluster_name>/<shard_number>/<database_name>/<backup_date>_<backup_type>/<table_name>.zip
# where backup_date is written in YYYYMMDD format and backup_type means full backup or incremental one.
#
# The script checks if any backup of a certain table  has already been made. 
# If false (backup table does not exist) then full backup is made no matter what.
# If any backup file was found then at least one full backup is made and the script will create an incremental one.
#------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------


#------------------------------------------------------------------------------------------------------------------------------
# Global variables
#------------------------------------------------------------------------------------------------------------------------------
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

# Get shard number for backup directory name
shard_num=$($docker clickhouse-client -u "$user" --password "$pass" \
	-q "SELECT shard_num
	    FROM system.clusters
	    WHERE host_name=hostName();")

# Get cluster name which is written in 'database' macros
cluster=$($docker clickhouse-client -u "$user" --password "$pass" \
	-q "SELECT getMacro('database');")

# Get date format for backup directory name and get the day of the month.
date=$(date +%Y%m%d)
month_day=$(date +%d)

#------------------------------------------------------------------------------------------------------------------------------
# Functions definition
#------------------------------------------------------------------------------------------------------------------------------
do_full_backup() { 
	$docker clickhouse-client -u "$user" --password "$pass" \
		-q "BACKUP TABLE "$database"."$1" TO Disk('backup_disk', '"$cluster"/"$shard_num"/"$database"/"$date"_full/"$1".zip');"
}

do_incremental_backup() {
	$docker clickhouse-client -u "$user" --password "$pass" \
		-q "BACKUP TABLE "$database"."$1" TO Disk('backup_disk', '"$cluster"/"$shard_num"/"$database"/"$date"_incr/"$1".zip')
			SETTINGS base_backup=Disk('backup_disk', '"$cluster"/"$shard_num"/"$database"/"$2"/"$1".zip');"
}

# Base backup is needed for incremental one.
get_base_backup() {
	$docker bash -c "ls -rt /data/chstore-backup/"$cluster"/"$shard_num/"$database"/*/"$1".zip" | tail -1" | awk -F"/" '{print $7}' 
}

# Get the number of backup files for a certain table.
num_backup_files() {
	$docker find /data/chstore-backup/"$cluster"/"$shard_num"/"$database" -name "$1.zip" | wc -l 
}


#------------------------------------------------------------------------------------------------------------------------------
# Start script
#------------------------------------------------------------------------------------------------------------------------------

main() {

	if [[ "$month_day" -eq 1 ]] ; then	# Check day of the month number
		
		for table in $tables
		do	
			do_full_backup "$table"
		done
	else
		for table in $tables
		do	
			if [[ $(num_backup_files "$table") -ne 0 ]] ; then	
									# Check if there is any backup file for every table.
									# If the file exists than create incremental backup.
				base_backup=$(get_base_backup "$table")
				do_incremental_backup "$table" "$base_backup"
			else
				do_full_backup "$table"
			fi	
		done
	fi
}

main
