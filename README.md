## Distributed Streaming Data Processing System
###### Author: Ziang Wan, Yan Xu
In this repository, Yan and I implement a distributed streaming data processing system. It consists of a membership
maintainer, a failure detector, a distributed file system and TCP-based application master and workers.

### How to run:

In the root directory
```
make
cd craneShellWorkspace
./craneShell
```

Then do shell input
```
join
submit timeDistribution
```

### Examples

To see sample crane applications: ./examples/
1. commentWordCount
2. timeDistribution
3. topTenUser

To see the spark files that perform the same jobs: ./examplesSpark/
1. commentWordCount
2. timeDistribution
3. topTenUser

### Report

There is a report in this folder that explains our system and design in full details.