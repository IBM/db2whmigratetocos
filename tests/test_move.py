from itertools import islice
from db2whmigratetocos.db2wh_db2_utilities import get_tables_under_tablespace_in_db2woc, get_tablespaces_in_block_and_cos, get_tbpsace_name_for_table,move_the_tables,create_a_log_directory_for_a_batch
from db2whmigratetocos.queries import LIST_TABLES_IN_SCHEMA
from db2whmigratetocos.main import move
from tests.test_util import conn_manually
import unittest
import json
from rich.console import Console


console = Console()


with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestTableMove(unittest.TestCase):

    def test_move_table_from_selected_tablespace(self):
        tbspace_list = get_tablespaces_in_block_and_cos(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        tbspace_list = [x.strip(' ') for x in tbspace_list]
        ts_tb_dict = {}
        skip_len = len(tbspace_list)/10   #Shortening the list to reduce execution time
        for i in range (len(tbspace_list)):
            if i == skip_len:
                i += skip_len
                continue
            if i < len(tbspace_list):
                expected_data = get_tables_under_tablespace_in_db2woc(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'],tbspace_list[i])
                if expected_data[2] > 0:
                    for tables_list in expected_data[1]:
                        if  "OBJSTORESPACE" not in tbspace_list[i] and not tables_list[1].startswith("IBM"):
                            schemaname = tables_list[1]
                            tablename = tables_list[0]
                            ts_tb_dict.setdefault(tbspace_list[i], []).append(tables_list[0])  

        dest_tbspace = "OBJSTORESPACE1"
        source_tbspace_list_all = list(ts_tb_dict.keys())
        source_tbspace_list = source_tbspace_list_all[2:]
        source_tbspace_final = ','.join(source_tbspace_list)
        print("final list:", source_tbspace_list)

        # try:
        #     result = move(vdata['Pwd'], vdata['Hostname'],source_tbspace_final)
        #     print("move results: ",result)
        # except TypeError:
        #     print("Do nothing")
        
        # tablespace_after_move = get_tbpsace_name_for_table(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'], tablename, schemaname)
        # print("tablespace name after move: ",tablespace_after_move)
        #self.assertEqual(dest_tbspace, tablespace_after_move, "Table space is not changed for table " + tablename)

    
if __name__ == "__main__":
    unittest.main()
