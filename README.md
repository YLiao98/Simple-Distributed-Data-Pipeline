# Assignment 4 Report

## Pre-requisites for running this assignment

1. Python (3.7+)
2. [Pytest](https://docs.pytest.org/en/stable/)
3. [Ray](https://ray.io)
4. [Jaeger] (https://opentracing.io/guides/python/quickstart/)

## Setting Up the Cluster

 1. To set up the master node of the server, mostly follow the steps https://osticket.massopen.cloud/kb/faq.php?id=17
 The OS I choosed was Ubuntu focal 20.04, and I'd recommend flavor that has at least 4GB RAM

 2. To set up cluster correctly, all nodes except master node should not associate with a floating IP

 3. To access the master node, type ```ssh ubuntu@<floating IP>``` to access master node

 4. To access other nodes, first go to https://kaizen.massopen.cloud/dashboard/project/instances/ and check corresponding instances' IPs. Inside master node's terminal, type ```ssh ubuntu@<node's ip>``` to access other nodes' terminals

 5. To setup environment to run, and you must do this *for every node*, first 

 ```bash
 sudo apt-get update
 ```
```bash
sudo apt update
```
then install pip,
```bash
sudo apt install python3-pip
```
after done installing pip, install ray, docker and jaeger-client

```bash
pip3 install ray
```
```bash
sudo apt install docker.io
```

```bash
pip3 install jaeger-client
```


you can check if your ray is installed setup by typing ```ray --version```.

6. follow the jaeger all-in-one docker installation provided in assignment 4 pdf file, for docker permission issues, check https://stackoverflow.com/questions/48957195/how-to-fix-docker-got-permission-denied-issue

7. To start Ray, first start your jaeger docker on the master node, and then type ```ray start --head --redis-port=8888```, the terminal would provide instructions on how to start ray on other nodes. Basically, on other nodes, type ```ray start --address=<address> --redis-password=<password>```

8. To check jaeger and ray UI dashboard, I'd recommend using ssh port forwarding. Basically, open a new terminal on your local machine, and then type 

```ssh -L 8888:localhost:8888 ubuntu@<floating IP>``` then you can access the ray dashboard on your preferred browser on your local machine! The same process goes for accessing the jaeger UI, the default port for jaeger UI is 16686

9. Last step, to transfer project files, you could do ```git clone``` on master node or using *scp* command to copy from your local machine to the master node. I used *scp* command because all my updated files are stored locally; ```git clone``` should work theoritically after you install git on master node.



## Running tasks of assignment
***You should only do this part if the previous section is completed and you verify that docker and ray are both running correctly and you can access UI from your local computer.***

For each operator, there is a flag corresponding to the operator to determine if this operator needs tracing or not

For instance, if we need to trace Scan operator,
```bash
--scan 1
```
if we turn off tracing the groupby operator
```bash
--groupby 2
```

For ```track_prov``` flag, turn it on and off follows the tracing of operators

### run tasks in terminal: 


```bash
$ python assignment_ray.py -t [task_number] -f [path_to_dataset] -r [path_to_ratings] -u [user_id] -sc [tracing_scan] -j [tracing_join] -gb [tracing_groupby] -ob [tracing_orderby] -tk [tracing_topk] -p [tracing_project] -tp [track_prov_flag]
```
For example, the following command runs the task2 with track_prov on

```bash
$ python assignment_ray.py -t 2 -f "../data/friends_short.txt" -r "../data/ratings.txt" -u 8 -sc 1 -j 1 -gb 1 -ob 1 -tk 1 -p 1 -tp 1
```


### Main Takeaways of Task 4

From screenshots, we can see that cluster takes much longer to finish than running locally, with both cluster and local machine process the same text files. Possible explanation for slower runtime could be internet connection, cpu computing power etc. One advantage of the cluster is that out of memory issues that happened on my local machine would not happen on the cluster, and we can process much larger files.
