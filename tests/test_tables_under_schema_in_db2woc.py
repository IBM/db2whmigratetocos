from db2whmigratetocos.db2wh_db2_utilities import get_tables_under_schema_in_db2woc,get_schema_in_instance
from db2whmigratetocos.queries import LIST_TABLES_IN_SCHEMA
from tests.test_util import conn_manually
import unittest
import json
from rich.console import Console


console = Console()


with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestTablesUnderSchema(unittest.TestCase):
    def test_valid_inputs(self):
        #table_names_in_schema = []
        schema_list = get_schema_in_instance(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        schema_list = [x.strip(' ') for x in schema_list]
        
        for schema in schema_list:
            actual_data = get_tables_under_schema_in_db2woc(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'],schema)
            if actual_data is not None:
                if actual_data[0] > 0:
                    self.assertTrue(actual_data[1] > 10, "Table size is too low for " + schema)
                    self.assertTrue(len(actual_data[2]) > 0, "Table count is incorrect for "+ schema)

        
    
if __name__ == "__main__":
    unittest.main()
