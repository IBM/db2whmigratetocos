from db2whmigratetocos.db2wh_db2_utilities import get_schema_in_instance
import unittest
import json

with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestGetConnectionString(unittest.TestCase):
    def test_valid_inputs(self):
        data = get_schema_in_instance(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        print(data)
        data = [x.strip(' ') for x in data]
        print(type(data))
        
if __name__ == "__main__":
    unittest.main()
