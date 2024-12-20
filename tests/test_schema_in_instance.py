from db2whmigratetocos.db2wh_db2_utilities import db2wh_pyodbc_connection, get_schema_in_instance
from db2whmigratetocos.queries import LIST_SCHEMAS
import unittest
import json
from tests.test_util import conn_manually

with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestSchemaList(unittest.TestCase):
    def test_valid_inputs(self):

        actual_data = get_schema_in_instance(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        actual_data = [x.strip(' ') for x in actual_data]
        self.assertTrue(len(actual_data) >= 1)
        expected_data = vdata['schema_list']
        self.assertTrue(len(actual_data) == len(set(actual_data)))   #check if all the schema name are unique
        self.assertTrue(set(actual_data).issuperset(set(expected_data)))    #check if DB2INST1 schema is present in the list 
    def test_invalid_inputs(self):
        raised = False
        try:
            actual_output = get_schema_in_instance(invdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], invdata['Database'])
            print("actual_output ", actual_output)
            self.assertIsNone(actual_output, 'Server Connected with incorrect value')
        except Exception as e: 
            raised = True
            self.assertTrue(raised, 'Exception raised')
    def test_data_compare(self):
        try:
            user_schemas_list = []
            rows = conn_manually(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'], LIST_SCHEMAS)
            for item in rows:
                if "SYS" not in item[0] and "NULL" not in item[0] and "SQL" not in item[0] and "IBMPDQ" not in item[0] and "DEFAULT" not in item[0]:
                    user_schemas_list.append(item[0])
            expected_list = [x.strip(' ') for x in user_schemas_list]
            # Write data jo JSON
            actual_data = get_schema_in_instance(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
            actual_data = [x.strip(' ') for x in actual_data]
            self.assertCountEqual(actual_data, expected_list, msg="List are not equal")
        except Exception as e:
            print(e)
    
    
if __name__ == "__main__":
    unittest.main()
