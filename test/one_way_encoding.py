# -*- coding: utf-8 -*-
"""
Created on Tue Jun 22 08:51:43 2021

@author: User
"""

import sys
import hashlib
import base64

if len(sys.argv) != 5:
        raise SystemExit("name_alg, root, extension, salt")

name_alg = sys.argv[1]
root = sys.argv[2]
extension = sys.argv[3]
salt = sys.argv[4]

name_alg = str.lower(name_alg.replace('-','',).replace('/','_')) if name_alg else 'sha1'
composite = '/'.join([str(root), str(extension)])
composite = salt + composite if salt else composite
buffer = composite.encode('UTF-8')
alg = getattr(hashlib, name_alg)()
alg.update(buffer)

print(base64.urlsafe_b64encode(alg.digest()).decode('UTF-8'))