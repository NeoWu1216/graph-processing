## Special Note
This is the final mp for cs425-Distributed System

## Contributed Project
We are using our old MP3 for file transfer during protocol  
We are using MP2 Best Solutions from group 57 in https://courses.engr.illinois.edu/cs425/fa2017/MP2Solutions.Python.zip (Thanks!)  
We are borrowing our old MP1 for debugging (i.e. grep logs)

## Language
We are using Python 2, so no compilation is required.  

## Execution
Start by calling `python main_driver.py` on several machines  
(*fa17-cs425-g48-01.cs.illinois.edu* or *fa17-cs425-g48-02.cs.illinois.edu* first)
It will run ls and show the following prompt:  
```
Input graph_file app_file app_args, or enter help:
```
So graph file is the input file that is in edge list format.  
app_file is the application file written in python defining AppVertex class that overrides Vertex and compute.  
app_args are the additional arguments passed to your application, which will be stored in self.app_args in your application.  
  
As an example:
```
Input graph_file app_file app_args, or enter help: com-amazon.ungraph.txt pr_vertex.py 20
```
  
There are also many flags provided, mainly for debugging purpose  

You need to at least keep 4 machines to start: 2 masters, 1 client and rest are workers.
Right after you input in one of the machines, that machine will be the client, and role will be assigned based on machine number for rest of machines.  
The progress will be checkpointed, so 2 simultaneous worker failures can be allowed without restart. 
