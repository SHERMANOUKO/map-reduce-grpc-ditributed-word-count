# DESIGN DESCRIPTION

In this project, we implement GRPC for connection between workers, driver and the client. To start the driver and workers, you have to specify which ports you want them to run on e.g.

```bash
python -m workerServer.py 4003 4004
```

will start twp worker servers on ports 4003 and 4004. You can start multiple workers and multiple drivers.

We use a threadpool executor (multiprocessing) to enable us handle multiple tasks at a go in a parallel manner. The map and reduce funrtions are all domiciled in the worker. The driver receives the request from the client. The driver then handles assigning of map and reduce tasks to available workers. It repeatedly checks to see if there's any available worker for it to assign tasks to the worker.

Once the map task is completed by the worker, it saves the intermediate file. Once the intermediate files have been created, the reducer can then work on them.

## Known errors

The client specifies the number of map and reduce tasks. This is set by the user when running the command

```bash
python client.py ./inputs 2 4 5000 5003
```

where 2 is the number of reduce tasks and 4 the number of map tasks. 5000 and 5003 represent the worker ports that have been opened indicating the servers ports where workers are running on. When creating the files, if the number of map tasks specified is more then the number of files being worked on, the outcome will not be as anticipated in the assignment qiestion. For example,

if files are 2 and map tasks specified as 4, the intermediate files will be (assuming reducers are 2) as shown below:

```bash
|-- mr-0-0
|-- mr-0-1
|-- mr-1-0
|-- mr-1-1
```

instead od 

```bash
|-- mr-0-0
|-- mr-0-1
|-- mr-1-0
|-- mr-1-1
|-- mr-2-0
|-- mr-2-1
|-- mr-3-0
|-- mr-3_1
```

I did have a dilema of whether or not to combine all the files before subdiving again to achieve the intended outcome. I thought it would probably create an unnecessary overhead.

## Feedback

Let me know how you'd improve this code via comments in this repo. There are inline comments in my code that offer a little more explanation.

Thanks for your time in looking at this.
