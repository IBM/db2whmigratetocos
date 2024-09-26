from db2whmigratetocos.db2wh_db2_utilities import get_connection_string
import unittest
import json

with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']
    # print(vdata)


class TestGetConnectionString(unittest.TestCase):
    def test_valid_inputs(self):
        expected_output_driver = vdata['Driver']
        expected_output_Database = vdata['Database']
        expected_output_Hostname = vdata['Hostname']
        expected_output_port = vdata['Port']
        expected_output_uid = vdata['Uid']
        expected_output_pwd = vdata['Pwd']
        expected_output_Security = vdata['Security']
        expected_output_Protocol = vdata['Protocol']

        conn_string = get_connection_string(
            vdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], vdata['Database'])
        conn_kvpair = self.get_con_kvpair(conn_string)
        # print(conn_kvpair.keys())

        self.assertIn(expected_output_driver, conn_kvpair.values())
        self.assertIn(expected_output_Database, conn_kvpair.values())
        self.assertIn(expected_output_Hostname, conn_kvpair.values())
        self.assertIn(expected_output_port, conn_kvpair.values())
        self.assertIn(expected_output_uid, conn_kvpair.values())
        self.assertIn(expected_output_pwd, conn_kvpair.values())
        self.assertIn(expected_output_Security, conn_kvpair.values())
        self.assertIn(expected_output_Protocol, conn_kvpair.values())

    def test_invalid_inputs(self):
        raised = False
        try:
            actual_output = get_connection_string(
                invdata['Uid'], vdata['Pwd'], vdata['Hostname'], vdata['Port'], invdata['Database'])
            print("actual_output " + actual_output)
            self.assertTrue(raised, 'Exception not raised')
        except TypeError:
            raised = True
            self.assertTrue(raised, 'Exception raised')

    def get_con_kvpair(self, connection_string: str):
        conn_list = connection_string.split(";")
        conn_list = ' '.join(conn_list).split()
        conn_kvpair = dict(s.split('=') for s in conn_list)
        return conn_kvpair


if __name__ == "__main__":
    unittest.main()
