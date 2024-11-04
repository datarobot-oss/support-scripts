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

"""
This module provides functions for comparing mongo database objects.
Usage:
Run this command from source before backup restore process or before upgrade:
`python3 data_consistency.py pre-upgrade`
Run this command from target after restore or upgrade:
`python3 data_consistency.py --post-upgrade --post-migration-pg-uri 'postgresql://username:password@pgsql_host'`
"""
# pylint: disable=W0141
# pylint: disable=W0601

import argparse
import os

import psycopg2


def get_databases(connection):
    """Get a list of all databases, excluding template0, template1, and postgres."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT datname FROM pg_database WHERE datname NOT IN ('template0', 'template1', 'postgres', 'rdsadmin');"
        )
        return [row[0] for row in cursor.fetchall()]


def get_table_info(connection):
    """Get table sizes and row counts for all tables in the public schema."""
    query = """
    SELECT
        c.relname AS table_name,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
        pg_total_relation_size(c.oid) / (current_setting('block_size')::integer / 1024) AS num_blocks,
        s.n_live_tup AS num_rows
    FROM
        pg_class c
    JOIN
        pg_stat_user_tables s ON c.oid = s.relid
    WHERE
        c.relkind = 'r'  -- Only include ordinary tables
    ORDER BY
        pg_total_relation_size(c.oid) DESC;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def get_index_count(connection):
    """Get the count of indexes for each table."""
    query = """
    SELECT tablename, count(indexname) 
    FROM pg_indexes 
    WHERE schemaname = 'public' 
    GROUP BY tablename;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def save_output_to_file(filename, databases):
    """Save the database output to a file."""
    output = []
    for db_name in databases:
        output.append(f"Database: {db_name}\n")

        # Connect to each database
        with psycopg2.connect(conn_str + f" dbname={db_name}") as db_connection:
            # Get table info
            table_info = get_table_info(db_connection)
            output.append("Table Info:\n")
            for row in table_info:
                if len(row) >= 4:  # Ensure we have all expected columns
                    output.append(f"{row}\n")

            # Get index count
            index_count = get_index_count(db_connection)
            output.append("Index Count:\n")
            for row in index_count:
                if len(row) >= 2:  # Ensure we have the table name and count
                    output.append(f"{row}\n")

    with open(filename, 'w') as f:
        f.writelines(output)


def compare_files(pre_file, post_file):
    """Compare pre-upgrade and post-upgrade files for discrepancies."""
    with open(pre_file, 'r') as f:
        pre_content = f.readlines()

    with open(post_file, 'r') as f:
        post_content = f.readlines()

    pre_tables = {}
    post_tables = {}

    # Parse pre-upgrade file
    current_db = ""
    for line in pre_content:
        if line.startswith("Database:"):
            current_db = line.strip().split(":")[1].strip()
        elif "Table Info:" in line or "Index Count:" in line:
            continue
        else:
            parts = line.strip().strip("()").split(", ")
            if len(parts) > 3:  # Ensure we have at least four elements
                name = parts[0]
                try:
                    count = int(parts[3])  # num_rows
                    pre_tables[(current_db, name)] = count
                except (IndexError, ValueError) as e:
                    print(f"Error parsing line in pre-upgrade file: {line.strip()} - {e}")

    # Parse post-upgrade file
    current_db = ""
    for line in post_content:
        if line.startswith("Database:"):
            current_db = line.strip().split(":")[1].strip()
        elif "Table Info:" in line or "Index Count:" in line:
            continue
        else:
            parts = line.strip().strip("()").split(", ")
            if len(parts) > 3:  # Ensure we have at least four elements
                name = parts[0]
                try:
                    count = int(parts[3])  # num_rows
                    post_tables[(current_db, name)] = count
                except (IndexError, ValueError) as e:
                    print(f"Error parsing line in post-upgrade file: {line.strip()} - {e}")

    # Compare the two sets
    discrepancies = []
    for (db, table), pre_count in pre_tables.items():
        post_count = post_tables.get((db, table))
        if post_count is not None and pre_count != post_count:
            discrepancies.append(
                f"Discrepancy in {db}.{table}: Pre-upgrade count {pre_count}, Post-upgrade count {post_count}"
            )

    return discrepancies


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Database pre-upgrade and post-upgrade check.')
    parser.add_argument('--pre-upgrade', action='store_true', help='Run pre-upgrade checks')
    parser.add_argument('--post-upgrade', action='store_true', help='Run post-upgrade checks')
    parser.add_argument('--post-migration-pg-uri', help='Post-migration PostgreSQL URI')
    args = parser.parse_args()

    PGSQL_HOST = os.environ.get('PGSQL_HOST')
    PGSQL_POSTGRES_PASSWORD = os.environ.get('PGSQL_POSTGRES_PASSWORD')
    PGSQL_USER = 'postgres'  # Default user

    if not PGSQL_HOST or not PGSQL_POSTGRES_PASSWORD:
        print("PGSQL_HOST and PGSQL_POSTGRES_PASSWORD must be set.")
        return

    # Connection string for pre-upgrade checks
    global conn_str
    conn_str = f"host={PGSQL_HOST} user={PGSQL_USER} password={PGSQL_POSTGRES_PASSWORD}"

    if args.pre_upgrade:
        try:
            with psycopg2.connect(conn_str) as connection:
                databases = get_databases(connection)
                save_output_to_file('/tmp/db_info_pre_upgrade.txt', databases)
                print("Pre-upgrade database information saved to /tmp/db_info_pre_upgrade.txt")

        except Exception as e:
            print(f"An error occurred: {e}")

    if args.post_upgrade and args.post_migration_pg_uri:
        # Connection string for post-upgrade checks
        post_conn_str = args.post_migration_pg_uri

        try:
            with psycopg2.connect(post_conn_str) as connection:
                databases = get_databases(connection)
                save_output_to_file('/tmp/db_info_post_upgrade.txt', databases)
                print("Post-upgrade database information saved to /tmp/db_info_post_upgrade.txt")

            discrepancies = compare_files(
                'db_info_pre_upgrade.txt', '/tmp/db_info_post_upgrade.txt'
            )
            if discrepancies:
                print("Discrepancies found:")
                for discrepancy in discrepancies:
                    print(discrepancy)
            else:
                print("No discrepancies found between pre-upgrade and post-upgrade counts.")

        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == '__main__':
    main()
