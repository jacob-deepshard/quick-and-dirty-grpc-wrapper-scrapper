import unittest
import threading
import grpc
import time
import tempfile
import os
import sys

from quick_and_dirty_grpc_wrapper_scrapper import start_grpc_server_from_python_script, fn
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf import message_factory

class TestQuickAndDirtyGrpcWrapperScrapper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a temporary Python script with functions to test
        cls.test_script_path = os.path.join(tempfile.gettempdir(), 'test_script.py')
        with open(cls.test_script_path, 'w') as f:
            f.write("""
from quick_and_dirty_grpc_wrapper_scrapper import fn

@fn
def greet(name):
    return f"Hello, {name}!"

@fn
def add_numbers(numbers):
    a, b = map(int, numbers.split(','))
    return str(a + b)

def not_exposed():
    return "This function is not decorated and should not be exposed."
""")

        # Start the gRPC server in a separate thread
        cls.server_thread = threading.Thread(
            target=start_grpc_server_from_python_script,
            args=(cls.test_script_path,),
            kwargs={'port': 50052},
            daemon=True
        )
        cls.server_thread.start()

        # Give the server a moment to start
        time.sleep(1)

        # Set up gRPC channel and factory
        cls.channel = grpc.insecure_channel('localhost:50052')
        cls.pool = DescriptorPool()
        cls.factory = message_factory.MessageFactory(cls.pool)

    @classmethod
    def tearDownClass(cls):
        # Clean up: Close the channel and remove the temporary script
        cls.channel.close()
        os.remove(cls.test_script_path)

    def test_greet_function(self):
        # Prepare the request message
        greet_input_type = self.pool.AddSerializedFile(b'').FindMessageTypeByName('dynamic_package.greetInput')
        greet_input = self.factory.GetPrototype(greet_input_type)()
        greet_input.data = 'TestUser'

        # Prepare the method
        method = self.channel.unary_unary(
            '/dynamic_package.DynamicService/greet',
            request_serializer=greet_input.SerializeToString,
            response_deserializer=self.factory.GetPrototype(
                self.pool.FindMessageTypeByName('dynamic_package.greetOutput')
            ).FromString,
        )

        # Call the method
        response = method(greet_input)
        self.assertEqual(response.data, 'Hello, TestUser!')

    def test_add_numbers_function(self):
        # Prepare the request message
        add_input_type = self.pool.AddSerializedFile(b'').FindMessageTypeByName('dynamic_package.add_numbersInput')
        add_input = self.factory.GetPrototype(add_input_type)()
        add_input.data = '5,7'

        # Prepare the method
        method = self.channel.unary_unary(
            '/dynamic_package.DynamicService/add_numbers',
            request_serializer=add_input.SerializeToString,
            response_deserializer=self.factory.GetPrototype(
                self.pool.FindMessageTypeByName('dynamic_package.add_numbersOutput')
            ).FromString,
        )

        # Call the method
        response = method(add_input)
        self.assertEqual(response.data, '12')

    def test_not_exposed_function(self):
        # Try to call a function that should not be exposed
        method = self.channel.unary_unary(
            '/dynamic_package.DynamicService/not_exposed',
            request_serializer=lambda x: x.SerializeToString(),
            response_deserializer=lambda x: x,
        )
        with self.assertRaises(grpc.RpcError) as context:
            method(b'')  # Empty request

        # Verify that the method is unimplemented
        self.assertEqual(context.exception.code(), grpc.StatusCode.UNIMPLEMENTED)

if __name__ == '__main__':
    unittest.main()