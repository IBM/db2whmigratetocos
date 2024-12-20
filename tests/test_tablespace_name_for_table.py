from db2whmigratetocos.db2wh_db2_utilities import get_tables_under_tablespace_in_db2woc, get_tablespaces_in_block_and_cos, get_tbpsace_name_for_table
import unittest
import json

with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

class TestTSNameForTable(unittest.TestCase):
    def test_valid_inputs(self):
        tbspace_list = get_tablespaces_in_block_and_cos(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        tbspace_list = [x.strip(' ') for x in tbspace_list]
        ts_tb_dict = {}
        skip_len = len(tbspace_list)/5   #Shortening the list to reduce execution time
        for i in range (len(tbspace_list)):
            if i == skip_len:
                i += skip_len
                continue
            if i < len(tbspace_list):
                expected_data = get_tables_under_tablespace_in_db2woc(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'],tbspace_list[i])
                if expected_data[2] > 0:
                    for tables_list in expected_data[1]:
                        ts_tb_dict.setdefault(tbspace_list[i], []).append(tables_list[0])

        # Testing with first tables space
        first_ts = next(iter(ts_tb_dict))
        actual_data1 = get_tbpsace_name_for_table(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'], ts_tb_dict[first_ts][0])
        self.assertEqual(actual_data1, first_ts, "Table space is incorrect for table " + ts_tb_dict[first_ts][0])

        # Testing with last tables space
        last_ts = next(reversed(ts_tb_dict))
        actual_data2 = get_tbpsace_name_for_table(vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'], ts_tb_dict[last_ts][0])
        self.assertEqual(actual_data2, last_ts, "Table space is incorrect for table " + ts_tb_dict[last_ts][0])

if __name__ == "__main__":
    unittest.main()