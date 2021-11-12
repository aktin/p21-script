import os
import time
from src.p21import import P21Importer

os.environ['username'] = 'i2b2crcdata'
os.environ['password'] = 'demouser'
os.environ['connection-url'] = 'jdbc:postgresql://localhost:5432/i2b2?searchPath=i2b2crcdata'
os.environ['uuid'] = '3fc5b451-3333-1245-1134-a70bfc58fd1f'
os.environ['script_id'] = 'p21import'
os.environ['script_version'] = '1.2'

path_parent = os.path.dirname(os.getcwd())
path_resources = os.path.join(path_parent, 'resources')
path_aktin_properties = os.path.join(path_resources, 'aktin.properties')
path_zip = os.path.join(path_resources, 'p21_verification.zip')
os.environ['path_aktin_properties'] = os.environ['path_aktin_properties'] = path_aktin_properties

start_time = time.time()

p21 = P21Importer(path_zip)
p21.verify_file()
print('--- %s seconds ---' % (time.time() - start_time))
p21.import_file()
print('--- %s seconds ---' % (time.time() - start_time))
