## Mongo consistency script usage example

Execute from any of the pods where python is available, preferrably from mmapp pods

### Pre-upgrade capture 

```
bash-4.4$ python3 /tmp/mongo_consistency.py pre-upgrade
All database object inventory before restore/upgrade collected successfully!
bash-4.4$ cd /tmp/
bash-4.4$ ls -ltrh
-rw-r--r-- 1 datarobot-service datarobot-service 9.1K Nov  4 00:59 mongo_consistency.py
-rw-r--r-- 1 datarobot-service datarobot-service  85K Nov  4 01:00 inventory_pre_upgrade.txt
```

### Post-upgrade verification

```
bash-4.4$ python3 /tmp/data_consistency.py post-upgrade --file=/tmp/inventory_pre_upgrade.txt --post-mongo-uri "mongodb://<username>:<password>@pcs-mongo-headless:27017/"


Collection: queue
--------------------------------------------------------------------------------
Number of Documents Difference: 6 -> 0 = 6
Safe to ignore collection: queue


Collection: queue_monitor
--------------------------------------------------------------------------------
Number of Documents Difference: 6679 -> 6688 = 9
Safe to ignore collection: queue_monitor


Collection: job_executions
--------------------------------------------------------------------------------
Size Difference: 2MB -> 2MB =0MB
Number of Documents Difference: 7971 -> 7980 = 9
Safe to ignore collection: job_executions


Collection: compute_cluster_metrics
--------------------------------------------------------------------------------
Number of Documents Difference: 404 -> 410 = 6
Safe to ignore collection: compute_cluster_metrics


Collection: execute_kubeworkers_health_checks
--------------------------------------------------------------------------------
Number of Documents Difference: 86465 -> 86582 = 117
Safe to ignore collection: execute_kubeworkers_health_checks


Collection: jobs
--------------------------------------------------------------------------------
Size Difference: 0MB -> 0MB =0MB

 There is one collection value that differs.
```

## Postgres consistency script usage example

Execute from any of the pods where python is available, preferrably from mmapp pods

### Pre-upgrade data capture

```
bash-4.4$ python3 /tmp/postgres_consistency.py --pre-upgrade
Pre-upgrade database information saved to /tmp/db_info_pre_upgrade.txt
bash-4.4$ cd /tmp/
bash-4.4$ ls -ltrh
-rw-r--r-- 1 datarobot-service datarobot-service 7.5K Nov  4 01:12 postgres_consistency.py
-rw-r--r-- 1 datarobot-service datarobot-service 9.4K Nov  4 01:13 db_info_pre_upgrade.txt
-rw-r--r-- 1 datarobot-service datarobot-service 9.4K Nov  4 01:15 db_info_post_upgrade.txt
```

### Post-upgrade consistency check

```
bash-4.4$ python3 /tmp/pg-data-consistency.py --post-upgrade --post-migration-pg-uri "postgresql://postgres:<password>@<postgres-service>:5432"
Post-upgrade database information saved to /tmp/db_info_post_upgrade.txt
Discrepancies found:
Discrepancy in sushihydra.'hydra_oauth2_access': Pre-upgrade count 9018, Post-upgrade count 9020
```
