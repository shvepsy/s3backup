# require python-configparser, python-boto3, mysql-client, gzip
import boto3, datetime, os, argparse, configparser


def s3init(aws_access_key_id,aws_secret_access_key,bucket_target):
    try:
        session = boto3.Session(aws_access_key_id = aws_access_key_id,aws_secret_access_key = aws_secret_access_key)
        s3 = session.resource('s3')
    except Exception as e:
        print "Cant connect to s3: " + str(e)
        exit(1)
    return s3.Bucket(bucket_target)

def mysql_backup(user,password,host,db,backup_dir):
    file_path =  backup_dir + "/" + db + ".bak.sql.gz"
    dumpcmd = "mysqldump -u" + user + " -p" + password + " -h" + host + " " + db + " 2>/dev/null | gzip -c > " + file_path
    try:
        os.system(dumpcmd)
    except Exception as e:
        print "Cant dump mysql: " + str(e)
        exit(1)
    return file_path


class s3Backups:
    def __init__(self, dumps, bucket_handler):
        get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
        self.dumps_count = dumps
        self.bucket = bucket_handler
        try:
     	    self.saved_dumps = self.bucket.objects.all()
            self.sorted_dumps = sorted(self.saved_dumps, key=get_last_modified, reverse=True)
        except Exception as e:
            print "Cant get dumps: " + str(e)
            exit(1)

    def list(self):
        for obj in self.sorted_dumps:
            print obj.key

    def empty(self):
        return len(self.sorted_dumps) < 0

    def pull(self,filename=''):
        if not self.empty():
            if not filename:
                 filename = self.sorted_dumps[0].key
            try:
                self.bucket.download_file(filename,os.path.basename(filename))
                return True
            except Exception as e:
                print "Cant pull: " + str(e)
        return False

    def push(self, filename):
    	if os.path.isfile(filename):
    	    dumpname = self.backup_rename(os.path.basename(filename))
    	    try:
    	        self.bucket.upload_file(filename, dumpname)
    	        return True
    	    except Exception as e:
                print "Cant push: " + str(e)
    	return False

    def rotation_possible(self):
        return len(list(self.saved_dumps)) - self.dumps_count > 0 and self.dumps_count > 0

    def rotate(self):
    	if self.rotation_possible():
    	    dumps_exceeding_count = len(list(self.saved_dumps)) - self.dumps_count
    	    exceeding_dumps = self.sorted_dumps[-dumps_exceeding_count:]
    	    for obj in exceeding_dumps:
    	        try:
                    obj.delete()
    		except Exception as e:
    		    print "Cant delete: " + str(e)
    		    break
    	    return True
    	return False

    def delete(self,filename):
    	if filename:
    	    try:
                res = filter(lambda x: x.key == filename,self.sorted_dumps)
                if len(res) > 0:
                    res[0].delete()
                return True
            except Exception as e:
                print "Cant delete: " + str(e)
    	return False

    def backup_rename(self,filename):
        if filename:
            timestamp = datetime.datetime.now().strftime("-%H:%M:%S-%d:%m:%Y")
            words = filename.split('.')
            words[0] = words[0] + timestamp
            backup_name = ".".join(words)
            return backup_name
        exit('Invalid filename')

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='s3 dumper')

    parser.add_argument('action', action='store',
                      choices=['backup','list','pull','delete'], help='Actions')

    parser.add_argument('filenames', action='store', nargs='*',
                        help='Filenames for push or delete actions')
    args = parser.parse_args()


    config = configparser.SafeConfigParser({'aws_count_backups': '7', 'db_host': '127.0.0.1', 'backup_dir': './backups'})

    config.read('backup.conf')
    if not 'AWS' in config:
        exit('AWS config not readed')

    dumps_count = config.getint('AWS','aws_count_backups')
    aws_access_key_id = config.get('AWS','aws_access_key_id')
    aws_secret_access_key = config.get('AWS','aws_secret_access_key')
    aws_bucket_name = config.get('AWS','aws_bucket_name')


    bucket = s3init(aws_access_key_id,aws_secret_access_key,aws_bucket_name)
    backups = s3Backups(dumps_count,bucket)

    if args.action == 'list':
        backups.list()

    elif args.action == 'pull':
        if args.filenames:
            for file in args.filenames:
                backups.pull(file)
        else:
            backups.pull()

    elif args.action == 'delete':
        if args.filenames:
            for file in args.filenames:
                backups.delete(file)
        else:
            print "Nothing to delete"

    elif args.action == 'backup':
        for opt in ['db_user','db_pass','db_names']:
            if not config.has_option('MYSQL', opt):
                print 'Not enough db params'
                exit(1)
        db_user = config.get('MYSQL','db_user')
        db_pass = config.get('MYSQL','db_pass')
        db_host = config.get('MYSQL','db_host')
        db_names = config.get('MYSQL','db_names').split(',')
        backup_dir = config.get('MYSQL','backup_dir')

        print "Backup started"

        for db_name in db_names:
            file = mysql_backup(db_user,db_pass,db_host,db_name,backup_dir)
            if os.path.isfile(file):
                backups.push(file)
            else:
                exit("File '%s' not created" % file)
        if backups.rotate():
            print "Cleanup success"
        else:
            print "Not enough dumps for cleanup"
        print "Backup success"

    else:
        print "Not implemented action"

    #
    # backups.list()
    #
    # if backups.push('222.tar.gz'):
    #     print "Push OK"
    #
    # backups.list()
    #
    # if backups.delete('222.tar.gz-09072018'):
    #     print "Delete OK"
    #
    # if backups.rotate():
    #     print "Rotate OK"
    #
    #
    #

#date = datetime.datetime.now().strftime("-%d%m%Y")

#file = '222.tar.gz'
#dumpname = file + date

#if os.path.isfile(file):
#    bucket.upload_file(file, dumpname)
#    print 'Dump saved as %s' % dumpname


#get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
#saved_dumps = bucket.objects.all()
#sorted_dumps = sorted(saved_dumps, key=get_last_modified)
# Download
#if len(sorted_dumps) > 0:
#    last_dump_key = sorted_dumps[0].key
#    bucket.download_file(last_dump_key,'lastbak.tar.gz')


# Rotation

#for obj in sorted_dumps:
#    print obj.key

#if len(list(saved_dumps)) > dumps_count - 1:
#    print "Require rotation. Deleting: "

#    dumps_exceeding_count = len(list(saved_dumps)) - dumps_count + 1
#    print dumps_exceeding_count
#    exceeding_dumps = []
#    if dumps_exceeding_count > 0:
#        exceeding_dumps = sorted_dumps[0:dumps_exceeding_count]

    #print [obj.key for obj in exceeding_dumps]
#    for obj in exceeding_dumps:
#        print obj.key
#        obj.get().delete()
#        obj.delete()
