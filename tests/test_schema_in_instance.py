from db2whmigratetocos.db2wh_db2_utilities import get_schema_in_instance
import unittest
import json

with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestSchemaList(unittest.TestCase):
    def test_valid_inputs(self):

        actual_data = get_schema_in_instance(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        actual_data = [x.strip(' ') for x in actual_data]
        self.assertTrue(len(actual_data) >= 1)
        expected_data = vdata['schema_name']
        self.assertTrue(len(actual_data) == len(set(actual_data)))   #check if all the schema name are uniq
        self.assertTrue(set(actual_data).issuperset(set(expected_data)))    #check if DB2INST1 schema is present in the list 
    def test_invalid_inputs(self):
        raised = False
        try:
            actual_output = get_schema_in_instance(
                invdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], invdata['Database'])
            print("actual_output " + actual_output)
            self.assertTrue(raised, 'Exception not raised')
        except TypeError:
            raised = True
            self.assertTrue(raised, 'Exception raised')

if __name__ == "__main__":
    unittest.main()
