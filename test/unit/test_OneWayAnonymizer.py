import unittest
from src.p21import import OneWayAnonymizer


class TestOneWayAnonymizer(unittest.TestCase):

    def test_anonymize_sha1(self):
        anonymizer = OneWayAnonymizer()
        hash1 = anonymizer.anonymize('root', 'ext', 'salt')
        self.assertEqual('2dIpFJQG_BflkTgkYYbHdnY59hM=', hash1)

    def test_anonymize_sha1_missing_salt(self):
        anonymizer = OneWayAnonymizer()
        hash1 = anonymizer.anonymize('root', 'ext', '')
        self.assertEqual('3OLW8Md-0-jVketgW0ovUVEji6g=', hash1)

    def test_anonymize_sha1_list(self):
        anonymizer = OneWayAnonymizer()
        list_ext = ['ext1', 'ext2', 'ext3']
        list_hash = anonymizer.anonymize_list('root', list_ext, 'salt')
        self.assertEqual('65AfbJVhzZIUDIsAPgeJ1AXtpAs=', list_hash[0])
        self.assertEqual('JfZlNG2-Z0WwPZZvhgZv8ww6Eqg=', list_hash[1])
        self.assertEqual('DGillUpgT9Yj0qWaVu1ruImghNI=', list_hash[2])

    def test_anonymize_md5(self):
        anonymizer = OneWayAnonymizer('MD5')
        hash1 = anonymizer.anonymize('root', 'ext', 'salt')
        self.assertEqual('5tKnuyzbBNCORTO9JxwIuQ==', hash1)
