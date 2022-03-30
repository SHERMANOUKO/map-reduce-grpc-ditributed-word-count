import grpc
import glob
import sys
import workers_pb2
import workers_pb2_grpc
from concurrent import futures
from collections import Counter
import time

class Worker(workers_pb2_grpc.WorkerServicer):

    def __init__(self):
        super().__init__()

    def setDriverPort(self, request, context):
        self.driver_port = request.port # retrieve driver port.
        return workers_pb2.status(code=200)

    def map(self, request, context):
        
        inputFile = request.filePath
        mapID = request.mapID
        noOfReducers = request.noOfReducers
        words = ""

        with open(inputFile, "r") as file:
            words = file.read().lower() 
            words = words.split()
        
        # remove "words" that are not pure alphabets
        # this is not an optimised solution since it will flag out words in brackets 
        # or with apostrophees for instance
        words = [word for word in words if word.isalpha()]

        # create a list of empty buckets for intermediate folder
        intermediate = [[] for i in range(noOfReducers)]
        
        # add words to respective buckets
        position = lambda x: ord(x) - 97 #we are using only lower case letters

        for word in words:
            bucketIndex = position(word[0]) % noOfReducers
            intermediate[bucketIndex].append(word)
        
        # print words to respective files
        for bucketId, intermediateFile in enumerate(intermediate):
            with open(f"./intermediate/mr-{mapID}-{bucketId}", "w+") as file:
                file.write("\n".join(intermediateFile))

        return workers_pb2.status(code=200)

    def reduce(self, request, context):
        
        reduceID = request.id
        
        # read from the intermediate folder files with this bucket / reduce id
        intermediateFiles = glob.glob(f"./intermediate/*-{reduceID}")
        
        # use counter for effective aggregation
        counter = Counter([])
        
        for file in intermediateFiles:
            words = ""
            
            with open(file, 'r') as file:
                words = file.read().split()
            
            counter.update(words)
        
        with open(f"./out/out-{reduceID}", 'w+') as file:
            file.write("\n".join(f"{word.capitalize()} {count}" for word, count in counter.items()))
         
        return workers_pb2.status(code=200)

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    workers_pb2_grpc.add_WorkerServicer_to_server(Worker(), server)

    workerPort = sys.argv[1] # pick port worker is supposed to run on as specified by user
    server.add_insecure_port(f"127.0.0.1:{workerPort}")
    server.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    server()
