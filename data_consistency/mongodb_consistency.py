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
`python3 data_consistency.py post-upgrade --file <file/path/inventory_pre_upgrade.txt>`
For remote mongo-uri connections:
Run this command from source before backup restore process or before upgrade:
`python3 data_consistency.py pre-upgrade --mongo-uri 'mongodb://username:password@mongo_host:27017'`
Run this command from target after restore or upgrade:
`python3 data_consistency.py post-upgrade
--file <file/path/inventory_pre_upgrade.txt>
--post-mongo-uri 'mongodb://username:password@mongo_host:27017'`
"""
# pylint: disable=W0141

from __future__ import absolute_import
from __future__ import division

import os

import click
from pymongo import MongoClient


@click.command()
@click.argument("mode", type=click.Choice(["pre-upgrade", "post-upgrade"]), required=True)
@click.option("-f", "--file", help="Inventory file for pre-upgrade mode")
@click.option("--post-mongo-uri", help="Post-upgrade MongoDB URI")
def data_consistency_check(mode=None, post_mongo_uri=None, file=None):
    if mode is None:
        click.echo("Please specify the mode: pre-upgrade or post-upgrade.")

    # Get MongoDB connection details from environment variables
    mongo_host = os.getenv("MONGO_HOST")
    mongo_user = os.getenv("MONGO_USER")
    mongo_password = os.getenv("MONGO_PASSWORD")
    mongo_connect_method = os.getenv("MONGO_CONNECT_METHOD")

    # Construct the MongoDB URI
    mongo_uri = f"{mongo_connect_method}://{mongo_user}:{mongo_password}@{mongo_host}"

    if mode == "pre-upgrade":
        get_inventory_pre_upgrade(mongo_uri)
    elif mode == "post-upgrade":
        if file is None:
            click.echo("Inventory file is required for post-upgrade mode.")
        else:
            get_inventory_post_upgrade(post_mongo_uri, file)
    else:
        click.echo("Invalid mode specified. Choose either pre-upgrade or post-upgrade.")


def get_inventory_pre_upgrade(mongo_uri):
    if mongo_uri is None:
        click.echo("Mongo URI is required.")
        return

    client = MongoClient(mongo_uri)
    databases = client.list_database_names()

    with open("/tmp/inventory_pre_upgrade.txt", "w") as file:
        for db_name in databases:
            if db_name in ["system", "local", "admin", "config"]:
                continue

            current_db = client[db_name]
            collections = current_db.list_collection_names()

            file.write(f"\n{'='*80}\nDatabase: {db_name}\n{'='*80}\n\n")

            for collection in collections:
                if collection.startswith("system."):
                    continue

                collection_size = current_db.command("collstats", collection)["storageSize"]
                num_docs = current_db[collection].estimated_document_count()
                indexes = current_db[collection].index_information()
                num_indexes = len(indexes)

                file.write(f"Collection: {collection}\n{'-'*80}\n")
                file.write(f"Size: {collection_size}\n")
                file.write(f"Number of Documents: {num_docs}\n")
                file.write(f"Number of Indexes: {num_indexes}\n")
                file.write(f"{'-'*80}\n\n")

    client.close()
    print("All database object inventory before restore/upgrade collected successfully!")


def get_inventory_post_upgrade(post_mongo_uri, file):
    if post_mongo_uri is None:
        click.echo("Post Mongo URI is required.")
        return

    client = MongoClient(post_mongo_uri)
    databases = client.list_database_names()

    with open("/tmp/inventory_post_upgrade.txt", "w") as inventory_file:
        for db_name in databases:
            if db_name in ["system", "local", "admin", "config"]:
                continue

            current_db = client[db_name]
            collections = current_db.list_collection_names()

            inventory_file.write(f"\n{'='*80}\nDatabase: {db_name}\n{'='*80}\n\n")

            for collection in collections:
                if collection.startswith("system."):
                    continue

                collection_size = current_db.command("collstats", collection)["storageSize"]
                num_docs = current_db[collection].estimated_document_count()
                indexes = current_db[collection].index_information()
                num_indexes = len(indexes)

                inventory_file.write(f"Collection: {collection}\n{'-'*80}\n")
                inventory_file.write(f"Size: {collection_size}\n")
                inventory_file.write(f"Number of Documents: {num_docs}\n")
                inventory_file.write(f"Number of Indexes: {num_indexes}\n")
                inventory_file.write(f"{'-'*80}\n\n")

    client.close()
    compare_inventories(file, "/tmp/inventory_post_upgrade.txt")


def parse_inventories(pre_file_name, post_file_name):
    with open(pre_file_name, "r") as pre_file:
        pre_inventory = pre_file.readlines()
    with open(post_file_name, "r") as post_file:
        post_inventory = post_file.readlines()

    pre_collections = parse_collection_info(pre_inventory)
    post_collections = parse_collection_info(post_inventory)

    return pre_collections, post_collections


def parse_collection_info(lines):
    collection_info = {}
    collection_name = ""
    for line in lines:
        if line.startswith("Collection:"):
            if collection_name:
                collection_info[collection_name] = info
            collection_name = line.split(":")[1].strip()
            info = {}
        elif line.startswith("Size:"):
            info["size"] = line.split(":")[1].strip()
        elif line.startswith("Number of Documents:"):
            info["num_docs"] = int(line.split(":")[1].strip())
        elif line.startswith("Number of Indexes:"):
            info["num_indexes"] = int(line.split(":")[1].strip())

    if collection_name:
        collection_info[collection_name] = info

    return collection_info


def compare_inventories(pre_file_name, post_file_name):
    pre_collections, post_collections = parse_inventories(pre_file_name, post_file_name)
    differences_count = 0

    for collection, pre_info in pre_collections.items():
        post_info = post_collections.get(collection)

        if post_info:
            differences = []

            pre_size_mb = int(pre_info["size"]) / (1024 * 1024)
            post_size_mb = int(post_info["size"]) / (1024 * 1024)
            if pre_size_mb != post_size_mb:
                size_diff_msg = (
                    "Size Difference: "
                    f"{round(pre_size_mb)}MB -> {round(post_size_mb)}MB ="
                    f"{round(abs(pre_size_mb - post_size_mb))}MB"
                )
                differences.append(size_diff_msg)

            if pre_info["num_docs"] != post_info["num_docs"]:
                docs_diff = pre_info["num_docs"] - post_info["num_docs"]
                docs_diff_msg = (
                    "Number of Documents Difference: "
                    f"{int(pre_info['num_docs'])} -> {int(post_info['num_docs'])} = "
                    f"{round(abs(docs_diff))}"
                )
                differences.append(docs_diff_msg)

            if pre_info["num_indexes"] != post_info["num_indexes"]:
                indexes_diff = pre_info["num_indexes"] - post_info["num_indexes"]
                indexes_diff_msg = (
                    "Number of Indexes Difference: "
                    f"{int(pre_info['num_indexes'])} -> {int(post_info['num_indexes'])} = "
                    f"{round(abs(indexes_diff))}"
                )
                differences.append(indexes_diff_msg)

            if differences:
                print("\n")
                print(f"Collection: {collection}")
                print("-" * 80)
                print("\n".join(differences))
                differences_count += 1

                if collection in [
                    "job_process",
                    "qid_counter",
                    "compute_cluster_metrics",
                    "queue_monitor",
                    "queue",
                    "job_executions",
                    "execute_kubeworkers_health_checks",
                ]:
                    print("Safe to ignore collection: " + collection)
                    differences_count -= 1

        else:
            print(f"Collection not present in post-upgrade inventory: {collection}")
            differences_count += 1

    if differences_count > 1:
        print("\n There are " + str(differences_count) + " collections that differ.")
    elif differences_count == 1:
        print("\n There is one collection value that differs.")
    else:
        print("\n There are no differences.")


if __name__ == "__main__":
    data_consistency_check()
