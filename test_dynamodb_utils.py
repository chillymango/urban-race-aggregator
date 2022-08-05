import unittest

from publish_to_dynamodb import make_db_item_from_dict


class TestDynamoDBUtils(unittest.TestCase):
    """
    Make sure this aggregate dict creation thing works
    """

    def test_string(self):
        KEY1 = "key1"
        VAL1 = "this is a test string"
        KEY2 = "key2"
        VAL2 = "another thing"
        str_kv = {KEY1: VAL1, KEY2: VAL2}
        output = make_db_item_from_dict(str_kv)
        self.assertDictEqual(output, {KEY1: {'S': VAL1}, KEY2: {'S': VAL2}})

    def test_num(self):
        KEY1 = "key1"
        VAL1 = 34.5
        KEY2 = "key2"
        VAL2 = 35
        num_kv = {KEY1: VAL1, KEY2: VAL2}
        output = make_db_item_from_dict(num_kv)
        self.assertDictEqual(output, {KEY1: {'N': "34.5"}, KEY2: {'N': "35"}})

    def test_bytes(self):
        KEY1 = "key1"
        VAL1 = bytes("thething", "utf-8")
        KEY2 = "key2"
        VAL2 = bytes("anotherthing", "utf-8")
        bytes_kv = {KEY1: VAL1, KEY2: VAL2}
        output = make_db_item_from_dict(bytes_kv)
        self.assertDictEqual(output, {KEY1: {'B': VAL1}, KEY2: {'B': VAL2}})


if __name__ == "__main__":
    unittest.main()
