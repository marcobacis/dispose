#Distributed stream processing
Implement a stream processing system for in-memory processing of streaming elements. The stream processing system exposes primitives to build a directed acyclic graph of operators. Each operator processes incoming elements and forwards the results to the downstream operators using a "pipeline" approach.

A special "supervisor" process exists. The supervisor exposes the primitives to build the operator graph. After the graph is built, the supervisor "deploys" it by instantiating a different process for each operator. Operator processes can be launched on different machines (to improve performance).

##Assumptions:

* The supervisor is reliable (but operators may fail).
* Input elements consist of numbers.
* A single operator type exists, which computes an aggregate over a count-based window. The user can specify the type of aggregation to perform (average, sum, min, max), the size, and the slide of the window.
* Operators are connected through FIFO (TCP) links.
* The output of an operator can be connected to multiple downstream operators (split), which receive a copy of the data produced upstream. Similarly, an operator may consume data from multiple upstream operators (join).
* There is a single source of input elements and a single consumer of output elements.
* The source can replay the input elements from a specified point.
* The system should implement fault tolerance (operators crashing). To do so, operators must periodically store their state into a reliable storage (you can assume it is the local disk and you can assume it does not fails). The supervisor monitors the state of the operators. If an operator fails, all operators must be brought back to the last reliable state and the source must be asked to replay all the elements from that state on.

##Additional notes:

* Operators are not partitioned (there is no data parallelism, only task/operator parallelism).
* The system is not required to guarantee that each result is delivered to the consumer exactly once in presence of failures.
* Implement the project in Java or simulate it in OmNet++.

###Optional
To get additional points you may choose to enable one of the following extensions:

* Consider input elements composed of key-value pairs and enable data parallelism.
* Guarantee that each result is delivered to the consumer exactly once.
