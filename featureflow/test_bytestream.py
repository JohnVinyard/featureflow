from bytestream import StringWithTotalLength
import unittest2


class StringWithTotalLengthTests(unittest2.TestCase):
    def test_left_add(self):
        self.assertEqual(
            'fakeblah', StringWithTotalLength('fake', 100) + 'blah')

    def test_right_add(self):
        self.assertEqual(
            'blahfake', 'blah' + StringWithTotalLength('fake', 100))

    def test_left_increment(self):
        x = StringWithTotalLength('fake', 100)
        x += 'blah'
        self.assertEqual('fakeblah', x)

    def test_right_increment(self):
        x = 'blah'
        x += StringWithTotalLength('fake', 100)
        self.assertEqual('blahfake', x)
