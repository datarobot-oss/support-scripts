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
# Latest master will have it in DataRobot/support/full_backup_script_onprem.py
# In older versions upto 9.x you can add this file manually with following instruction
# This script is not intended for 8.x dr versions

# Copy to host machine
# scp -i ~/.ssh/your_key.pem /path/to/DataRobot/tools/full_backup_script.py \
# ubuntu@your.host.ip.address:/tmp

# SSH to machine where k8s cluster exists
# ssh -i ~/.ssh/your_key.pem ubuntu@your.host.ip.address

# Below script takes backup of of the following:

# Configuration
# Secrets
# PostgreSQL
# MongoDB
# Usage: Copy the script, please make sure to pass DR_NAMESPACE value as argument and the BACKUP_LOCATION which would be the backup directory that has been created to store backups

# Example: full-backup-script.py my-test-namespace /datarobot-backup

# Please note: This script does not take backups of custom certificates and elasticsearch.
####################################################################################################

# pylint: disable=W0141

import os
import subprocess
import sys
import time
from datetime import datetime
import tarfile

def create_backup_directory(backup_location):
    os.makedirs(backup_location, exist_ok=True)

def backup_helm_values(namespace,backup_location):
    subprocess.run(f"helm get values -n {namespace} dr > {backup_location}/dr_values.yaml", shell=True, check=True)
    subprocess.run(f"helm get values -n {namespace} pcs > {backup_location}/pcs_values.yaml", shell=True, check=True)

def backup_secrets(namespace, backup_location):
    os.makedirs(f"{backup_location}/secrets", exist_ok=True)
    
    subprocess.run(f"kubectl -n {namespace} get secret/core-credentials -o jsonpath=\"{{.data.asymmetrickey}}\" | base64 -d > {backup_location}/secrets/ASYMMETRIC_KEY_PAIR_MONGO_ENCRYPTION_KEY.txt", shell=True, check=True)
    subprocess.run(f"kubectl -n {namespace} get secret/core-credentials -o jsonpath=\"{{.data.drsecurekey}}\" | base64 -d > {backup_location}/secrets/DRSECURE_MONGO_ENCRYPTION_KEY.txt", shell=True, check=True)

    os.makedirs(f"{backup_location}/secrets/pcs", exist_ok=True)
    for secret in subprocess.check_output(f"kubectl -n {namespace} get secrets -l app.kubernetes.io/instance=pcs -o name", shell=True).decode().strip().split('\n'):
        subprocess.run(f"kubectl -n \"{namespace}\" get \"{secret}\" -o json | jq '{{data}}' > \"{backup_location}/secrets/pcs/{secret.split('/')[-1]}.json\"", shell=True)

    os.makedirs(f"{backup_location}/secrets/certs", exist_ok=True)
    try:
        subprocess.run(f"kubectl -n {namespace} get secret rabbit-cert -o jsonpath='{{.data.*}}' > {backup_location}/secrets/certs/rabbitmq_certs.crt", shell=True, check=True)
    except subprocess.CalledProcessError:
        print("Warning: Could not retrieve rabbit-cert secrets. It may not exist.")


def backup_postgres(namespace, backup_location):
    pg_backup_location = os.path.join(backup_location, "pgsql")
    os.makedirs(pg_backup_location, exist_ok=True)
    os.environ['BACKUP_LOCATION'] = pg_backup_location
    os.environ['LOCAL_PGSQL_PORT'] = '54321'

    pg_password_cmd = f"kubectl -n {namespace} get secret pcs-postgresql -o jsonpath='{{.data.postgres-password}}' | base64 -d"
    pg_password = subprocess.check_output(pg_password_cmd, shell=True).decode().strip()
    os.environ['PGPASSWORD'] = pg_password
    print(f"PostgreSQL Password: {pg_password}")

    port_forward_cmd = f"kubectl -n {namespace} port-forward svc/pcs-postgresql --address 127.0.0.1 {os.environ['LOCAL_PGSQL_PORT']}:5432 &"
    subprocess.Popen(port_forward_cmd, shell=True)

    while True:
        try:
            subprocess.check_output(f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -c 'SELECT 1'", shell=True)
            print("PostgreSQL is ready to accept connections.")
            break
        except subprocess.CalledProcessError:
            print("Waiting for PostgreSQL to be ready...")
            time.sleep(10)

    dbs = subprocess.check_output(f"psql -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -t -c 'SELECT datname FROM pg_database;' | grep -vE 'template|repmgr|postgres' | sed 's/\\r//g'", shell=True).decode().strip().splitlines()
    print(f"Databases available for backup: {dbs}")

    create_db_file_path = os.path.join(pg_backup_location, 'create_databases.sql')
    with open(create_db_file_path, 'w') as create_db_file:
        for db in dbs:
            db = db.strip()
            if db:
                create_db_file.write(f"CREATE DATABASE {db} WITH OWNER {db};\n")

    for db in dbs:
        db = db.strip()
        if db:
            db_backup_path = os.path.join(pg_backup_location, db)
            os.makedirs(db_backup_path, exist_ok=True)

            # Backup schema only
            schema_backup_cmd = f"pg_dump -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -Fp --schema-only {db} -f {db_backup_path}/schema.sql"
            print(f"Backing up schema for database: {db}")
            subprocess.run(schema_backup_cmd, shell=True, check=True)

            cpu_count = os.cpu_count()

            data_backup_cmd = f"pg_dump -Upostgres -hlocalhost -p{os.environ['LOCAL_PGSQL_PORT']} -j{cpu_count} -Z0 -Fd  {db} -f {db_backup_path}/data"
            print(f"Backing up data for database: {db}")
            subprocess.run(data_backup_cmd, shell=True, check=True)

    port_forward_pid_cmd = f"ps aux | grep -E 'port-forwar[d].*{os.environ['LOCAL_PGSQL_PORT']}' | awk '{{print $2}}'"
    port_forward_pid = subprocess.check_output(port_forward_pid_cmd, shell=True).decode().strip()
    if port_forward_pid:
        os.kill(int(port_forward_pid), 15)

    current_date = datetime.now().strftime("%F")
    tar_file_path = os.path.join(backup_location, f"pgsql-backup-{current_date}.tar")
    with tarfile.open(tar_file_path, "w") as tar:
        tar.add(pg_backup_location, arcname=os.path.basename(pg_backup_location))


def main(namespace, backup_location):
    os.environ['NAMESPACE'] = namespace
    os.environ['BACKUP_LOCATION'] = backup_location
    os.environ['LOCAL_MONGO_PORT'] = '27018'

    mongo_passwd_cmd = f"kubectl -n {namespace} get secret pcs-mongo -o jsonpath='{{.data.mongodb-root-password}}' | base64 -d"
    mongo_passwd = subprocess.check_output(mongo_passwd_cmd, shell=True).decode().strip()
    os.environ['MONGO_PASSWD'] = mongo_passwd

    port_forward_cmd = f"kubectl -n {namespace} port-forward svc/pcs-mongo-headless --address 127.0.0.1 {os.environ['LOCAL_MONGO_PORT']}:27017 &"
    subprocess.Popen(port_forward_cmd, shell=True)

    os.makedirs(f"{backup_location}/mongodb", exist_ok=True)

    mongodump_cmd = f"mongodump -vv -u pcs-mongodb -p {mongo_passwd} -h 127.0.0.1 --port {os.environ['LOCAL_MONGO_PORT']} -o {backup_location}/mongodb"
    dump_process = subprocess.Popen(mongodump_cmd, shell=True)

    while dump_process.poll() is None:
        time.sleep(360)

    port_forward_pid_cmd = f"ps aux | grep -E 'port-forwar[d].*{os.environ['LOCAL_MONGO_PORT']}' | awk '{{print $2}}'"
    port_forward_pid = subprocess.check_output(port_forward_pid_cmd, shell=True).decode().strip()

    if port_forward_pid:
        os.kill(int(port_forward_pid), 15)

    tar_cmd = f"tar -cf datarobot-mongo-backup-$(date +%F).tar -C {backup_location} mongodb"
    subprocess.run(tar_cmd, shell=True, check=True)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python backup_script.py <NAMESPACE> <BACKUP_LOCATION>")
        sys.exit(1)

    namespace_arg = sys.argv[1]
    backup_location_arg = sys.argv[2]

    create_backup_directory(backup_location_arg)
    backup_helm_values(namespace_arg, backup_location_arg)
    backup_secrets(namespace_arg, backup_location_arg)
    backup_postgres(namespace_arg, backup_location_arg)
    main(namespace_arg, backup_location_arg)
