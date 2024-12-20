from db2whmigratetocos.db2wh_db2_utilities import db2wh_pyodbc_connection, get_tablespaces_in_block_and_cos
from db2whmigratetocos.queries import LIST_TBSPACES
import unittest
import json
from tests.test_util import conn_manually

with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestTSList(unittest.TestCase):
    def test_valid_inputs(self):

        actual_data = get_tablespaces_in_block_and_cos(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        actual_data = [x.strip(' ') for x in actual_data]
        self.assertTrue(len(actual_data) >= 1)
        expected_data = vdata['tablespace_list']
        print("expected data is:", expected_data)
        self.assertTrue(len(actual_data) == len(set(actual_data)))   #check if all the Tablespaces name are unique
        self.assertTrue(set(actual_data).issuperset(set(expected_data)))    #check if USERSPACE1 and OBJSTORESPACE1 TBSPACE is present in the list 

    def test_invalid_inputs(self):
        raised = False
        try:
            actual_output = get_tablespaces_in_block_and_cos(invdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], invdata['Database'])
            self.assertIsNone(actual_output, 'Server Connected with incorrect value')
        except Exception as e: 
            raised = True
            self.assertTrue(raised, 'Exception raised')
    def test_data_compare(self):
        try:
            user_tablespaces_list = []
            
            rows = conn_manually(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'], LIST_TBSPACES.format(TABLESPACE=tablespace))
            for item in rows:
                if "SYS" not in item[0] and "TS4CONSOLE" not in item[0] and "BIGSQLCATUTILITY" not in item[0] and "TEMP" not in item[0] and "TMP" not in item[0]:
                    user_tablespaces_list.append(item[0])
            expected_list = user_tablespaces_list
            expected_list = [x.strip(' ') for x in user_tablespaces_list]
            actual_data = get_tablespaces_in_block_and_cos(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
            actual_data = [x.strip(' ') for x in actual_data]
            self.assertCountEqual(actual_data, expected_list, msg="List are not equal")
        except Exception as e:
            print(e)
        
    
if __name__ == "__main__":
    unittest.main()
