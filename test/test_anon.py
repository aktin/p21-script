# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 17:54:41 2021

@author: User
"""

import os
import hashlib
import base64

def get_aktin_property(property_aktin):
    if not os.path.exists(PATH_AKTIN_PROPERTIES):
        raise SystemExit('file path for aktin.properties is not valid')
    with open(PATH_AKTIN_PROPERTIES) as properties:
        for line in properties:
            if "=" in line:
                key, value = line.split("=", 1)
                if(key == property_aktin):
                    return value.strip()
        return ""

def one_way_anonymizer(name_algo, root, extension, salt):
    name_algo = str.lower(name_algo.replace('-','',).replace('/','_'))
    composite = '/'.join([str(root), str(extension)])
    composite = salt + composite if salt else composite

    buffer = composite.encode('UTF-8')
    algo = getattr(hashlib, name_algo)()
    algo.update(buffer)
    return base64.urlsafe_b64encode(algo.digest()).decode('UTF-8')


PATH_AKTIN_PROPERTIES = r'C:\Users\User\Desktop\aktin.properties'


root = "A"
extension = "B"
name_alg = get_aktin_property("pseudonym.algorithm")
salt = get_aktin_property("pseudonym.salt")


print(one_way_anonymizer(name_alg, root, extension, salt))


