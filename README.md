### s3backup

# HOWTO backup mysql database to s3

* Install required python-configparser, python-boto3, mysql-client, gzip
* Edit backup.conf 
* Start script 

# Usage 

```
python backup.py backup			# backup database and save 
python backup.py list			# list bucket dumps
python backup.py pull			# download last backup
python backup.py pull <backupname>	# download specific backup
python backup.py delete  <backupname>	# delete specific backup
```

