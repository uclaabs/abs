# ABS: A System for Scalable Approximate Queries with Accuracy Guarantees

ABS is a parallel approximate query processing for running interactive SQL queries over massive data.
It allows users to pick the desire spot in the latency-accuracy space by running the queries data samples, and presents approximate query results with accuracy guarantees.

ABS achieves this by exploiting the recent advance in scalable error estimation techniques -- **Analytical Bootstrap Method**,
and lets the compiler automatically choose this fast method whenever possible and brings the error estimation overhead down to seconds.

ABS is available for **Hive** and **Shark**, and supports HiveQL.

ABS is currently built on top of

+ Hive 0.11
+ Shark 0.9.1
+ Spark 0.9.1

Note that ABS is different from the sequential implementation ABM presented in our SIGMOD [paper](http://dl.acm.org/citation.cfm?id=2588555.2588579), which is implemented as a middle layer on top of MonetDB using Java and R. 

### Standalone Setup

ABS can be set up very quickly using standalone mode.

1. Clone the source
2. Enter abs root folder `cd abs`
3. Run clean script `chmod +x clean.sh && ./clean.sh`
4. Compile using `sbt/sbt compile`

After you have successfully compiled your abs code, you can start ABS cli by
`./bin/shark` or start server by `./bin/shark --service sharkserver`

### Cluster Setup

ABS requires Spark 0.9.1 for running on clusters.
You can setup the cluster by following similar steps in [Running Shark on a Cluster](https://github.com/amplab/shark/wiki/Running-Shark-on-a-Cluster)

ABS extends Hive and Shark. This repository contains all the codes for Shark and for Hive, we provide a jar file: hive-exec-0.11.0-shark-0.9.1.jar. If you are interested in Hive implementation, please check [here](https://github.com/uclaabs/absHive).

### Related Papers

Kai Zeng, Shi Gao, Jiaqi Gu, Barzan Mozafari, Carlo Zaniolo: [ABS: a system for scalable approximate queries with accuracy guarantees.](http://dl.acm.org/citation.cfm?id=2594532) ACM SIGMOD 2014 (Best Demo Award)

Kai Zeng, Shi Gao, Barzan Mozafari, Carlo Zaniolo: [The analytical bootstrap: a new method for fast error estimation in approximate query processing.](http://dl.acm.org/citation.cfm?id=2588555.2588579) ACM SIGMOD 2014

Kai Zeng, Shi Gao, Barzan Mozafari, Carlo Zaniolo: The analytical bootstrap: a new method for fast error estimation in approximate query processing. Technical Report CSD #130028, UCLA, 2013.





