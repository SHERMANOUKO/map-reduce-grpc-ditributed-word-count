# Writing a few unittests for this project
# Tests use the inbuilt python unittest module

import unittest
from unittest import mock

from workerServer import Worker

# create a request object
driver_request = mock.Mock()
driver_request.port = 5000

map_request = mock.MagicMock()
map_request.filePath = "./inputs/test_file.txt"
map_request.mapID = 1
map_request.noOfReducers = 2

reduce_request = mock.MagicMock()
reduce_request.id = 1

mock_file_open = mock.mock_open()

def mocked_file_opening(*args, **kwargs):
    # print(fileName)
    # print(mode)
    return

class TestWorker(unittest.TestCase):

    # test whether the super method in proto generated python files is called
    # helps us confirm that we are inheriting from those files
    @mock.patch("workers_pb2_grpc.WorkerServicer.__init__")
    def test_super_method(self, mock_super):
        Worker().setDriverPort(request=driver_request, context=None)
        self.assertTrue(mock_super.called)
    
    # test that driver port function works
    def test_driver_port(self):
        response = Worker().setDriverPort(request=driver_request, context=None)
        self.assertEqual(response.code,200)

    # test that an exception is raised if no driver port is provided
    def test_missing_driver_port(self):
        with self.assertRaises(AttributeError) :
            Worker().setDriverPort(request=None, context=None)

    # test whether the map function calls the file opening method
    @mock.patch("workerServer.open")
    def test_map(self, mock_file_open):
        response = Worker().map(request=map_request, context=None)
        self.assertGreaterEqual(mock_file_open.call_count, 1) # should be called atleast once
        self.assertEqual(response.code,200)
        self.assertTrue(mock_file_open.called)

if __name__ == '__main__':
    unittest.main()
