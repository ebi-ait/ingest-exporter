# [Dead Letter Queues](https://www.rabbitmq.com/dlx.html)

We need to declare our new dead letter queues and add `x-dead-letter-exchange` and `x-dead-letter-routing-key` arguments to the existing queues, which unfortunately cannot have their arguments edited. 

Therefore: to add these arguments to an existing queue, the queue must be **deleted**. The exporter will then automatically recreate any missing queues and bindings as part of the queue declaration process configured in [exporter.py](../exporter.py)


1) Connect the remote rabbit admin port to your local machine:
```.shell
kubectl port-forward rabbit-0 5672:5672
```
3) run the python script [setup_dlq.py](./setup_dlq.py):
```.shell
python ./setup_dlq.py
```
5) You can now end the port forward for the admin port and instead connect to the web admin port to ensure that the queues have been defined appropriately.
```.shell
kubectl port-forward rabbit-0 15672:15672
```
 ![](./DLQ_Screenshot.png)