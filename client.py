
import sys
from urllib import response
import grpc

import driver_pb2_grpc
import driver_pb2

def run():

    channel = grpc.insecure_channel("127.0.0.1:5000")

    try:
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError:
        sys.exit("Error connecting to server")
    else:
        args =  sys.argv # fecth all arguments passed through terminal

        stub = driver_pb2_grpc.DriverStub(channel)

        workerPorts = ','.join(args[4:]) #fetch all ports with active workers

        request = driver_pb2.processData(
            filesDirectory=args[1],
            noOfReducers=int(args[2]),
            noOfMappers=int(args[3]),
            ports=workerPorts
        )

        print(stub.startDriver(request))

if __name__ == "__main__":
    run()
