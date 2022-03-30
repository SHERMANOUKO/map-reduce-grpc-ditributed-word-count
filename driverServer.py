from concurrent import futures
import glob
import sys
import time
import grpc
import driver_pb2_grpc
import driver_pb2
import workers_pb2
import workers_pb2_grpc

class Driver(driver_pb2_grpc.DriverServicer):

    def __init__(self) -> None:
        super().__init__()
        self.workers = {}
    
    def retrieveAvailableWorker(self):
        for k, v in self.workers.items():
            if v[0] == 0:
                return k # return worker port if worker is available
        return False
 
    def mapTasks(self, workerPort, file, mapId, noOfReducers):
        self.workers[workerPort][0] = 1 # mark worker as busy
        
        try:
            request = workers_pb2.mapMetadata(filePath=file, mapID=mapId, noOfReducers=noOfReducers)
        except Exception as e:
            print(str(e)) # Logging would be ideal. Just doing simple printing for now
        
        response = self.workers[workerPort][1].map(request) # call the map method using the stub
        
        if response.code != 200:
            print("Error in response")  # Logging would be ideal. Just doing simple printing for now
        
        self.workers[workerPort][0] = 0 # release worker

        return 

    def reduceTasks(self, workerPort, reducerID):
        self.workers[workerPort][0] = 1 # mark worker as busy
        
        try:
            request = workers_pb2.reducerMetadata(id=reducerID)
        except Exception as e:
            print(str(e)) # Logging would be ideal. Just doing simple printing for now
        
        response = self.workers[workerPort][1].reduce(request)
        
        if response.code != 200:
            print("Error in response")  # Logging would be ideal. Just doing simple printing for now

        self.workers[workerPort][0] = 0 # release worker
        
        return

    def startDriver(self, request, context):
        # driver port
        driverPort = int(sys.argv[1])

        # no of reduce tasks and map tasks
        noOfReduceTasks = request.noOfReducers
        noOfMapTasks = request.noOfMappers

        # retrieve and store files
        inputFiles = glob.glob(f"{request.filesDirectory}/*.txt")
        
        # retrieve worker ports
        workerPorts = [int(workerPort) for workerPort in request.ports.split(',')]
        
        # create connection to worker ports
        for port in workerPorts:
            
            channel = grpc.insecure_channel(f"127.0.0.1:{port}")

            try:
                grpc.channel_ready_future(channel).result(timeout=10)
            except grpc.FutureTimeoutError:
                print(f"Error occured")  # Logging would be ideal. Just doing simple printing for now

            self.workers[port] = [0, workers_pb2_grpc.WorkerStub(channel)] # store stub object and state for later use.
            request = workers_pb2.driverPort(port=driverPort) # create request object
            statusCode = self.workers[port][1].setDriverPort(request) # retrieve stub object and send request
            
            if statusCode.code != 200:
                print("Error in setting driver port")  # Logging would be ideal. Just doing simple printing for now
        
        # use a context manager to handle execution of thread pools
        # this helps with memory and garbage collection
        with futures.ThreadPoolExecutor() as executor:
            mapIdCounter = 0
            
            for file in inputFiles:

                if mapIdCounter >= noOfMapTasks:
                    mapIdCounter = 0

                workerAvailable = self.retrieveAvailableWorker()

                while workerAvailable == False:
                    time.sleep(3) # sleep and wait for a worker to be free
                    workerAvailable = self.retrieveAvailableWorker()
                
                executor.submit(self.mapTasks, workerPort=workerAvailable, file=file, mapId=mapIdCounter, noOfReducers=noOfReduceTasks)

                mapIdCounter+=1
        
        with futures.ThreadPoolExecutor() as executor:
        
            for reduceID in range(noOfReduceTasks):
                             
                workerAvailable = self.retrieveAvailableWorker()
                
                while workerAvailable == False:
                    
                    time.sleep(3) # sleep and wait for a worker to be free
                    workerAvailable = self.retrieveAvailableWorker()
                
                executor.submit(self.reduceTasks, workerPort=workerAvailable, reducerID=reduceID)
        
        return driver_pb2.status(code=200)

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    driver_pb2_grpc.add_DriverServicer_to_server(Driver(), server)

    # get driver port
    port = sys.argv[1]
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    server()
