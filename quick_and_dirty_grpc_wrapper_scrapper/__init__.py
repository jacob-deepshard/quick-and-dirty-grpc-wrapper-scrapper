import importlib.util
import inspect
import grpc
from concurrent import futures
import sys
import os
import types
from google.protobuf import descriptor_pool, descriptor_pb2, message_factory
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf import reflection

def extract_all_top_level_functions(module):
    """
    Extracts all top-level functions from a given module.
    Returns a dictionary of function names to function objects.
    """
    functions = {}
    for name, obj in inspect.getmembers(module, inspect.isfunction):
        # Ensure the function is defined in the module (not imported)
        if obj.__module__ == module.__name__:
            functions[name] = obj
    return functions

def create_grpc_server(module, port=50051):
    """
    Creates and starts a gRPC server exposing the functions from the module.
    """
    functions = extract_all_top_level_functions(module)

    # Dynamically create protobuf definitions
    pool = descriptor_pool.Default()
    factory = message_factory.MessageFactory(pool)

    # Create a FileDescriptorProto
    file_descriptor_proto = descriptor_pb2.FileDescriptorProto()
    file_descriptor_proto.name = 'dynamic.proto'
    package_name = 'dynamic_package'
    file_descriptor_proto.package = package_name

    service_descriptor = file_descriptor_proto.service.add()
    service_descriptor.name = 'DynamicService'

    # Message types
    message_types = {}

    for func_name, func in functions.items():
        # Assuming all functions take a dictionary as input and return a dictionary
        # Create input and output message types
        input_type_name = f'{func_name}Input'
        output_type_name = f'{func_name}Output'

        for type_name in [input_type_name, output_type_name]:
            if type_name not in message_types:
                message_descriptor = file_descriptor_proto.message_type.add()
                message_descriptor.name = type_name
                # For simplicity, we'll assume all messages have a generic 'data' field of type string
                field = message_descriptor.field.add()
                field.name = 'data'
                field.number = 1
                field.label = FieldDescriptor.LABEL_OPTIONAL
                field.type = FieldDescriptor.TYPE_STRING
                message_types[type_name] = message_descriptor

        # Add method to service
        method = service_descriptor.method.add()
        method.name = func_name
        method.input_type = f'.{package_name}.{input_type_name}'
        method.output_type = f'.{package_name}.{output_type_name}'

    # Build the FileDescriptor
    file_descriptor = pool.Add(file_descriptor_proto)

    # Create service class
    service_descriptor = file_descriptor.services_by_name['DynamicService']

    class DynamicServiceServicer:
        # Dynamically add methods to the servicer
        pass

    for func_name, func in functions.items():
        input_type = factory.GetPrototype(
            file_descriptor.message_types_by_name[f'{func_name}Input']
        )
        output_type = factory.GetPrototype(
            file_descriptor.message_types_by_name[f'{func_name}Output']
        )

        def make_method(func, input_type, output_type):
            def method(self, request, context):
                # Deserialize input
                input_data = request.data
                # Call the actual function
                result = func(input_data)
                # Serialize output
                response = output_type()
                response.data = str(result)
                return response
            return method

        method = make_method(func, input_type, output_type)
        setattr(DynamicServiceServicer, func_name, method)

    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Register servicer
    service = grpc.method_handlers_generic_handler(
        f'{package_name}.DynamicService',
        {func_name: grpc.unary_unary_rpc_method_handler(
            getattr(DynamicServiceServicer, func_name),
            request_deserializer=factory.GetPrototype(
                file_descriptor.message_types_by_name[f'{func_name}Input']
            ).FromString,
            response_serializer=factory.GetPrototype(
                file_descriptor.message_types_by_name[f'{func_name}Output']
            ).SerializeToString,
        ) for func_name in functions}
    )
    server.add_generic_rpc_handlers((service,))

    # Start the server
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"gRPC server started on port {port}")
    server.wait_for_termination()

def start_grpc_server_from_python_script(script_path, port=50051):
    """
    Loads a Python script as a module and starts a gRPC server to expose its functions.
    """
    module_name = os.path.splitext(os.path.basename(script_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    create_grpc_server(module, port)
