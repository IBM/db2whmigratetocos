from db2whmigratetocos.db2wh_db2_utilities import get_tables_under_tablespace_in_db2woc,get_tablespaces_in_block_and_cos,tab_size_by_table_name
from db2whmigratetocos.queries import LIST_TABLES_IN_TSPACE
from tests.test_util import conn_manually
import unittest
import json
from rich.console import Console


console = Console()


with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestTablesUnderTS(unittest.TestCase):
    def test_valid_inputs(self):
        #table_names_in_tablespace = []
        tbspace_list = get_tablespaces_in_block_and_cos(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        tbspace_list = [x.strip(' ') for x in tbspace_list]
        for tbspace in tbspace_list:
            # expected_data = conn_manually(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'], LIST_TABLES_IN_TSPACE.format(TABLESPACE=tbspace))
            # with console.status(""):
            #     for item in expected_data:
            #         if "SYS" not in item[1]:
            #             if str(item[0]).endswith('t') is False:
            #                 table_cnt = table_cnt + 1
            #                 est_size = tab_size_by_table_name(
            #                     vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'], item[1], item[0])
            #                 total_estimate_size += int(est_size)
            #                 table_names_in_tablespace.append([item[0], item[1], est_size])

            actual_data = get_tables_under_tablespace_in_db2woc(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'],tbspace)
            if actual_data[2] > 0:
                self.assertTrue(actual_data[0] > 10, "Table size is too low for " + tbspace)
                self.assertTrue(len(actual_data[1]) > 0, "Table count is incorrect for "+ tbspace)
        
    
if __name__ == "__main__":
    unittest.main()
