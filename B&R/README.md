## Backup script usage guide

Usage: Copy the script, please make sure to pass `DR_NAMESPACE` value as argument and the `BACKUP_LOCATION` which would be the backup directory that has been created to store backups

Example: `python3 full-backup-script.py <my-test-namespace> /datarobot-backup`

Copy to host machine
```
scp -i ~/.ssh/your_key.pem /path/to/DataRobot/tools/full_backuo_script_onprem.py \
ubuntu@your.host.ip.address:/tmp
```

 SSH to machine where k8s cluster exists and execute the script using example usage above
 
`ssh -i ~/.ssh/your_key.pem ubuntu@your.host.ip.address`

Backup cript takes backup of of the following:

1. Configuration
2. Secrets
3. PostgreSQL
4. MongoDB

_Please note: This script does not take backups of custom certificates and elasticsearch._

## Restore script usage guide

Usage: Copy the script, please make sure to pass `DR_NAMESPACE` value as argument and the `BACKUP_LOCATION` where the backups are stored

 Example: `python3 db_restore_script.py <my-test-namespace> /datarobot-backup`

 Copy to host machine where k8s cluster is running
```
scp -i ~/.ssh/your_key.pem /path/to/DataRobot/tools/db_restore_script_onprem.py \
 ubuntu@your.host.ip.address:/tmp
```

 SSH to machine and execute the script using example above
 
`ssh -i ~/.ssh/your_key.pem ubuntu@your.host.ip.address`
