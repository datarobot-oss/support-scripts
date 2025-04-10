#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2021 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc. Confidential.
#
# This is unpublished proprietary source code of DataRobot, Inc.
# and its affiliates.
#
# The copyright notice above does not evidence any actual or intended
# publication of such source code.
####################################################################################################
# In older versions upto 9.x you can add this file manually with following instruction
# This script is not intended for 8.x dr versions

# Copy to host machine
# scp -i ~/.ssh/your_key.pem /path/to/DataRobot/tools/db_restore_script.py \
# ubuntu@your.host.ip.address:/tmp

# SSH to machine where k8s cluster exists
# ssh -i ~/.ssh/your_key.pem ubuntu@your.host.ip.address

# Below script takes restore the following:

# PostgreSQL or  MongoDB or Both
#
# Usage: Copy the script, please make sure to pass DR_NAMESPACE value as argument, the BACKUP_LOCATION which would be the backup directory that has been created to store backups and postgres or mongodb as arguments for selective database varient to restore.
# For Help:
#          python db_restore_script.py --help
# Usage Example:
#          python db_restore_script.py my-test-namespace /absolute-datarobot-backup-location postgres  # for PostgreSQL only restore
#          python db_restore_script.py my-test-namespace /absolute-datarobot-backup-location mongodb     # for MongoDB only restore
#          python db_restore_script.py my-test-namespace /absolute-datarobot-backup-location complete     # for both PostgreSQL and MongoDB databases restore

# Please note: This script does not restore any other components other than databases
####################################################################################################

# pylint: disable=W0141

import os
import subprocess
import sys
import time
import logging
import json
import tarfile
import shutil
import argparse


def extract_database_names(output):
    try:
        json_line = next(line for line in output.strip().splitlines() if line.strip().startswith("["))
        dbs = json.loads(json_line)
        db_names = [db['name'] for db in dbs]
        return db_names
    except (StopIteration, json.JSONDecodeError) as e:
        logging.error(f"Error extracting database names: {e}")
        return []

def is_port_forwarded(port):
    try:
        output = subprocess.check_output(f"netstat -an | grep {port}", shell=True)
        return True if output else False
    except subprocess.CalledProcessError:
        return False

def wait_for_mongodb(mongo_user, mongo_passwd):
    while True:
        try:
            subprocess.check_output(
                f'mongosh  "mongodb://{mongo_user}:{mongo_passwd}@localhost:{os.environ["LOCAL_MONGO_PORT"]}/?directConnection=true&serverSelectionTimeoutMS=30000&authSource=admin" --eval "db.runCommand({{ ping: 1 }})"',
                shell=True
            )
            logging.info("MongoDB is ready to accept connections.")
            break
        except subprocess.CalledProcessError:
            logging.info("Waiting for MongoDB to be ready...")
            time.sleep(5)

def delete_pgsql_directory(backup_location):
    print("\nCleanup the tar extracted backup directory 'pgsql'. Read below carefully and respond...! If you are not sure type 'no' when prompted. ")
    time.sleep(5)
    print("\n\n NOTE: This optional and if you don't have recent backup 'pgsql-backup-<DATE>.tar' file, don't delete 'pgsql' directory since it is the only backup..")
    time.sleep(10)
    print("\n\n If you want to delete 'pgsql' directory type 'yes' otherwise type 'no' in the below prompt.")
    time.sleep(5)
    confirmation = input(f"\n\nDo you want to delete the 'pgsql' directory  ? (yes/no): ").lower()
    if confirmation == 'yes':
        print("\nDeleting pgsql driectroy upon confirmation")
        time.sleep(5)
        shutil.rmtree(os.path.join(backup_location,'pgsql'))
        print("\n'pgsql' directory removed after restore")
    else:
        print(f"Directory 'pgsql' was not deleted. Please handle it manually")

def delete_mongodb_directory(backup_location):
    print("\nCleanup the tar extracted backup directory 'mongodb'. Read below carefully and respond...! If you are not sure type 'no' when prompted.")
    time.sleep(5)
    print("\n\n NOTE: This optional and if you don't have recent backup 'datarobot-mongo-backup-<DATE>.tar' file, don't delete 'mongodb' directory since it is the only backup..")
    time.sleep(10)
    print("\n\n If you want to delete 'mongodb' directory type 'yes' otherwise type 'no' in the below prompt.")
    time.sleep(5)
    confirmation = input(f"\n\nDo you want to delete the 'mongodb' directory  ? (yes/no): ").lower()
    if confirmation == 'yes':
        print("\nDeleting mongodb driectroy upon confirmation")
        time.sleep(5)
        shutil.rmtree(os.path.join(backup_location,'mongodb'))
        print("\n'mongodb' directory removed after restore")
    else:
        print(f"Directory 'mongodb' was not deleted. Please handle it manually")

def mongo_restore(namespace, backup_location):

    print("Now MongoDB being restored...\n")
    os.environ['NAMESPACE'] = namespace
    os.environ['BACKUP_LOCATION'] = backup_location
    os.environ['LOCAL_MONGO_PORT'] = '27018'
    os.environ['LOCAL_PGSQL_PORT'] = '54321'

    os.chdir(backup_location)

    tar_file = subprocess.check_output("ls *datarobot-mongo-backup*.tar", shell=True).decode().strip()

    subprocess.run(f"tar xf {tar_file}", shell=True, check=True)

    mongo_passwd_cmd = f"kubectl -n {namespace} get secret pcs-mongo -o jsonpath='{{.data.mongodb-root-password}}' | base64 -d"
    mongo_passwd = subprocess.check_output(mongo_passwd_cmd, shell=True).decode().strip()
    os.environ['MONGO_PASSWD'] = mongo_passwd

    port_forward_mongo_cmd = f"kubectl -n {namespace} port-forward svc/pcs-mongo-headless --address 127.0.0.1 {os.environ['LOCAL_MONGO_PORT']}:27017 &"
    subprocess.Popen(port_forward_mongo_cmd, shell=True)

    cpu_count = os.cpu_count()

    mongorestore_cmd = f"mongorestore -vv --drop -j{cpu_count} --numInsertionWorkersPerCollection=6  -u pcs-mongodb -p {mongo_passwd} -h 127.0.0.1 --port {os.environ['LOCAL_MONGO_PORT']} {os.environ['BACKUP_LOCATION']}/mongodb"
    restore_process = subprocess.Popen(mongorestore_cmd, shell=True)

    while restore_process.poll() is None:
        time.sleep(100)  # Sleep for 5 minutes to avoid busy-waiting

    # Cleanup port forwarding process
    mongo_port_forward_pid_cmd = f"ps aux | grep -E 'port-forwar[d].*{os.environ['LOCAL_MONGO_PORT']}' | awk '{{print $2}}'"
    mongo_port_forward_pid = subprocess.check_output(mongo_port_forward_pid_cmd, shell=True).decode().strip()

    if mongo_port_forward_pid:
        os.kill(int(mongo_port_forward_pid), 15)  # Send SIGTERM

def postgres_restore(namespace, backup_location):
    # Add logic for PostgreSQL restore here
    print("Now PostgreSQL being restored...\n")
    pg_password_cmd = f"kubectl -n {namespace} get secret pcs-postgresql -o jsonpath='{{.data.postgres-password}}' | base64 -d"
    pg_password = subprocess.check_output(pg_password_cmd, shell=True).decode().strip()
    os.environ['PGPASSWORD'] = pg_password
    os.environ['NAMESPACE'] = namespace
    os.environ['BACKUP_LOCATION'] = backup_location
    os.environ['LOCAL_MONGO_PORT'] = '27018'
    os.environ['LOCAL_PGSQL_PORT'] = '54321'

    port_forward_pg_cmd = f"kubectl -n {namespace} port-forward svc/pcs-postgresql --address 127.0.0.1 {os.environ['LOCAL_PGSQL_PORT']}:5432 &"
    subprocess.Popen(port_forward_pg_cmd, shell=True)

    while True:
        try:
            subprocess.check_output(f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -c 'SELECT 1'", shell=True)
            print("PostgreSQL is ready to accept connections.")
            break
        except subprocess.CalledProcessError:
            print("Waiting for PostgreSQL to be ready...")
            time.sleep(5)  # Check every 5 seconds
    os.chdir(backup_location)
    tar_file = None
    for file in os.listdir():
        if "pgsql" in file and file.endswith(".tar"):
            tar_file = os.path.join(file)
            break
    if tar_file:
        print(f"Found tar file: {tar_file}")
        with tarfile.open(tar_file, "r") as tar:
            tar.extractall(path=os.path.join(backup_location))
            print(f"Extracted {tar_file} to {os.path.join('pgsql')}")
    for db in os.listdir("pgsql"):
        db_path = os.path.join("pgsql", db)
        if os.path.isdir(db_path) and db not in ['postgres', 'sushihydra', 'identityresourceservice']:
            print(f"Cleaning up database: {db}")

            check_db_cmd = f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -lqt | cut -d | -f 1 | grep -qw {db}"
            db_exists = subprocess.run(check_db_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


            clean_sql_command = """
            DO \\$$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END \\$$;
            """

            clean_sql_command_2 = """
            DO \\$$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = '_prediction_result_partitions') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END \\$$;
            """

            cleanup_cmd_1 = f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -d {db} -c \"{clean_sql_command}\""
            cleanup_cmd_2 = f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -d {db} -c \"{clean_sql_command_2}\""

            try:
                subprocess.run(cleanup_cmd_1, shell=True, check=True)
                print(f"Successfully cleaned up database: {db}")
            except subprocess.CalledProcessError as e:
                print(f"Error cleaning up database {db}: {e}")

            try:
                subprocess.run(cleanup_cmd_2, shell=True, check=True)
                print(f"Successfully cleaned up partition tables in database: {db}")
            except subprocess.CalledProcessError as e:
                print(f"Error cleaning up partition tables in database {db}: {e}")

    cleanup_sql_cmd_3 = """
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = 'modmon'
    AND pid <> pg_backend_pid();
    """

    cleanup_cmd_3 = f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -d postgres -c \"{cleanup_sql_cmd_3}\""
    #cleanup_cmd_4 = f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -d postgres -c \"drop database modmon\""
    subprocess.run(cleanup_cmd_3, shell=True, check=True)

    for db in os.listdir("pgsql"):
        db_path = os.path.join("pgsql", db)
        if os.path.isdir(db_path) and db not in ['postgres', 'sushihydra', 'identityresourceservice']:

            data_backup_path = os.path.join(db_path, 'data')
            print(f"Restoring data for database: {db} from {data_backup_path}")

            if os.path.exists(data_backup_path):
                cpu_count = os.cpu_count()
            try:
                restore_data_cmd = f"pg_restore -j{cpu_count} -v -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -c -d {db} \"{data_backup_path}\""
                subprocess.run(restore_data_cmd, shell=True, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Warning: Already exists or do not exist errors ignored on restore")
            else:
                print(f"Data backup path does not exist: {data_backup_path}")

    pg_port_forward_pid_cmd = f"ps aux | grep -E 'port-forwar[d].*{os.environ['LOCAL_PGSQL_PORT']}' | awk '{{print $2}}'"
    pg_port_forward_pid = subprocess.check_output(pg_port_forward_pid_cmd, shell=True).decode().strip()

    if pg_port_forward_pid:
        os.kill(int(pg_port_forward_pid), 15)

def main():
    # Initialize ArgumentParser
    parser = argparse.ArgumentParser(description="Database Restore Script Help")

    # Add arguments
    parser.add_argument('namespace', help="Please provide Kubernetes Namespace.")
    parser.add_argument('backup_location', help="Please provide absolute backup path.")
    parser.add_argument('db_to_be_restored', help="Database type to be restored (complete or postgres or mongodb), choices=['complete', 'postgres', 'mongodb'].")

    # Parse arguments
    args = parser.parse_args()

    # Print parsed arguments (optional)
    print(f"Namespace: {args.namespace}")
    print(f"Backup Location: {args.backup_location}")
    print(f"Database to be restored: {args.db_to_be_restored}")

    # Conditional logic for restoring MongoDB or PostgreSQL
    if args.db_to_be_restored == 'mongodb':
        print("Only MongoDB will be restored\n")
        mongo_restore(args.namespace, args.backup_location)
        delete_mongodb_directory(args.backup_location_arg)
    elif args.db_to_be_restored == 'postgres':
        print("Only PostgreSQL will be restored\n")
        postgres_restore(args.namespace, args.backup_location)
        delete_pgsql_directory(args.backup_location_arg)
    elif args.db_to_be_restored == 'complete':
        print("Both MongoDB and PostgreSQL databases will be restored\n")
        mongo_restore(args.namespace, args.backup_location)
        postgres_restore(args.namespace, args.backup_location)
        delete_mongodb_directory(args.backup_location)
        delete_pgsql_directory(args.backup_location)
    else:
        print("Please choose the database you would like to restore (mongodb/postgres/complete)")

if __name__ == "__main__":
    # Run the main function directly
    main()
