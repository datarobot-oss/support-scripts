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

# PostgreSQL
# MongoDB
# Usage: Copy the script, please make sure to pass DR_NAMESPACE value as argument and the BACKUP_LOCATION which would be the backup directory that has been created to store backups

# Example: db_restore_script.py my-test-namespace /datarobot-backup-location

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

def extract_database_names(output):
    try:
        json_line = next(line for line in output.strip().splitlines() if line.strip().startswith("["))
        
        dbs = json.loads(json_line)
        db_names = [db['name'] for db in dbs]
        return db_names
    except (StopIteration, json.JSONDecodeError) as e:
        logging.error(f"Error extracting database names: {e}")
        return []

def cleanup_mongodb(namespace):
    os.environ['NAMESPACE'] = namespace
    os.environ['LOCAL_MONGO_PORT'] = '27018'

    mongo_passwd_cmd = f"kubectl -n {namespace} get secret pcs-mongo -o jsonpath='{{.data.mongodb-root-password}}' | base64 -d"
    mongo_passwd = subprocess.check_output(mongo_passwd_cmd, shell=True).decode().strip()
    mongo_user = "pcs-mongodb"  # Assuming this is the username; adjust if necessary

    if not is_port_forwarded(os.environ['LOCAL_MONGO_PORT']):
        port_forward_cmd = f"kubectl -n {namespace} port-forward svc/pcs-mongo-headless --address 127.0.0.1 {os.environ['LOCAL_MONGO_PORT']}:27017 &"
        subprocess.Popen(port_forward_cmd, shell=True)
        time.sleep(5)

    wait_for_mongodb(mongo_user, mongo_passwd)

    try:
        dbs_output = subprocess.check_output(
            f"mongosh --username {mongo_user} --password {mongo_passwd} --host localhost --port {os.environ['LOCAL_MONGO_PORT']} --eval 'JSON.stringify(db.getMongo().getDBs().databases)'",
            shell=True
        ).decode().strip()

        logging.info(f"Raw output for databases: {dbs_output}")  # Log raw output

        db_names = extract_database_names(dbs_output)
        db_names = [db for db in db_names if db not in ['admin', 'local', 'config', 'system']]

        logging.info(f"Database names to clean: {db_names}")  # Log cleaned database names
    except subprocess.CalledProcessError as e:
        logging.error(f"Error occurred while fetching databases: {e.output.decode()}")
        return

    # Execute cleanup script for each database
    for db_name in db_names:
        logging.info(f"Cleaning up database: {db_name}")
        cleanup_script = f"""
        const currentDb = db.getSiblingDB('{db_name}');
        const collections = currentDb.getCollectionNames();
        collections.forEach(function(collectionName) {{
            print('Dropping collection: ' + collectionName);
            currentDb[collectionName].drop();
        }});
        """

        try:
            subprocess.run(
                f"mongosh --username {mongo_user} --password {mongo_passwd} --authenticationDatabase admin --host localhost --port {os.environ['LOCAL_MONGO_PORT']} --eval \"{cleanup_script}\"",
                shell=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to clean up database {db_name}: {e}")

    logging.info("MongoDB cleanup completed.")

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
                f"mongosh --username {mongo_user} --password {mongo_passwd} --port {os.environ['LOCAL_MONGO_PORT']} --host localhost --eval 'db.runCommand({{ ping: 1 }})'",
                shell=True
            )
            logging.info("MongoDB is ready to accept connections.")
            break
        except subprocess.CalledProcessError:
            logging.info("Waiting for MongoDB to be ready...")
            time.sleep(5)


def main(namespace, backup_location):
    
    cleanup_mongodb(namespace)
    os.environ['NAMESPACE'] = namespace
    os.environ['BACKUP_LOCATION'] = backup_location
    os.environ['LOCAL_MONGO_PORT'] = '27018'
    os.environ['LOCAL_PGSQL_PORT'] = '54321'

    tar_file = subprocess.check_output("ls *mongo-backup*.tar", shell=True).decode().strip()

    subprocess.run(f"tar xf {tar_file}", shell=True, check=True)

    cleanup_mongodb(namespace)

    mongo_passwd_cmd = f"kubectl -n {namespace} get secret pcs-mongo -o jsonpath='{{.data.mongodb-root-password}}' | base64 -d"
    mongo_passwd = subprocess.check_output(mongo_passwd_cmd, shell=True).decode().strip()
    os.environ['MONGO_PASSWD'] = mongo_passwd

    port_forward_mongo_cmd = f"kubectl -n {namespace} port-forward svc/pcs-mongo-headless --address 127.0.0.1 {os.environ['LOCAL_MONGO_PORT']}:27017 &"
    subprocess.Popen(port_forward_mongo_cmd, shell=True)

    cpu_count = os.cpu_count()

    mongorestore_cmd = f"mongorestore -vv -j{cpu_count} --nsExclude=admin.system.users --nsExclude=config.system.preimages --nsExclude=config.system --nsExclude=MMApp.job_process --nsExclude=MMApp.queue --nsExclude=MMApp.qid_counter --nsExclude=MMApp.queue_monitor --nsExclude=MMApp.execute_kubeworkers_health_checks --nsExclude=MMApp.execute_base_docker_images --numInsertionWorkersPerCollection=6  -u pcs-mongodb -p {mongo_passwd} -h 127.0.0.1 --port {os.environ['LOCAL_MONGO_PORT']} {backup_location}/mongodb"
    restore_process = subprocess.Popen(mongorestore_cmd, shell=True)

    while restore_process.poll() is None:
        time.sleep(300) 

    mongo_port_forward_pid_cmd = f"ps aux | grep -E 'port-forwar[d].*{os.environ['LOCAL_MONGO_PORT']}' | awk '{{print $2}}'"
    mongo_port_forward_pid = subprocess.check_output(mongo_port_forward_pid_cmd, shell=True).decode().strip()

    if mongo_port_forward_pid:
        os.kill(int(mongo_port_forward_pid), 15)  # Send SIGTERM

    pg_password_cmd = f"kubectl -n {namespace} get secret pcs-postgresql -o jsonpath='{{.data.postgres-password}}' | base64 -d"
    pg_password = subprocess.check_output(pg_password_cmd, shell=True).decode().strip()
    os.environ['PGPASSWORD'] = pg_password

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
    tar_file = None
    for db in os.listdir(os.path.join(backup_location, "pgsql")):
        db_path = os.path.join(backup_location, "pgsql", db)
        if os.path.isdir(db_path) and db not in ['postgres', 'sushihydra', 'identityresourceservice']:
            print(f"Cleaning up database: {db}")

            check_db_cmd = f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -lqt | cut -d \| -f 1 | grep -qw {db}"
            db_exists = subprocess.run(check_db_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


            clean_sql_command = """
            DO \$\$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END \$\$;
            """
    
            clean_sql_command_2 = """
            DO \$\$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = '_prediction_result_partitions') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END \$\$;
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

    tar_file = None
    for file in os.listdir(os.path.join(backup_location, "pgsql")):
        if "pgsql" in file and file.endswith(".tar"):
            tar_file = os.path.join(backup_location, "pgsql", file)
            break
    
    if tar_file:
        print(f"Found tar file: {tar_file}")
    
        with tarfile.open(tar_file, "r") as tar:
            tar.extractall(path=os.path.join(backup_location, "pgsql"))
            print(f"Extracted {tar_file} to {os.path.join(backup_location, 'pgsql')}")
    
    for db in os.listdir(os.path.join(backup_location, "pgsql")):
        db_path = os.path.join(backup_location, "pgsql", db)
        if os.path.isdir(db_path) and db not in ['postgres', 'sushihydra', 'identityresourceservice']:
            
            data_backup_path = os.path.join(db_path, 'data')
            print(f"Restoring data for database: {db} from {data_backup_path}")
               
            if os.path.exists(data_backup_path):
                cpu_count = os.cpu_count()
            try:
                restore_data_cmd = f"pg_restore -j{cpu_count} -v -c -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -d {db} \"{data_backup_path}\""
                subprocess.run(restore_data_cmd, shell=True, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Warning: Already exists or do not exist errors ignored on restore")
            else:
                print(f"Data backup path does not exist: {data_backup_path}")



    pg_port_forward_pid_cmd = f"ps aux | grep -E 'port-forwar[d].*{os.environ['LOCAL_PGSQL_PORT']}' | awk '{{print $2}}'"
    pg_port_forward_pid = subprocess.check_output(pg_port_forward_pid_cmd, shell=True).decode().strip()

    if pg_port_forward_pid:
        os.kill(int(pg_port_forward_pid), 15)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python db_restore_script.py <NAMESPACE> <BACKUP_LOCATION>")
        sys.exit(1)

    namespace_arg = sys.argv[1]
    backup_location_arg = sys.argv[2]

    main(namespace_arg, backup_location_arg)
