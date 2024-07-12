<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# JPacificA

[中文](./README_zh_CN.md)

## Overview
JPacificA is a production-grade high-performance log replication framework implemented in Java based on the consistency algorithm PacificA. 
Unlike other "majority" algorithms such as paxos, raft, zab, etc., 
it adopts a Quorum mechanism (W+R>N) of N+1>N, that is, 
"all nodes" are required for write requests, and only one node is required for read requests, 
which is very friendly for "read" performance scenarios. 
JPacificA is a general framework that you can use to implement multiple data replicas in your application, 
allowing for high availability of read services and consistency across multiple data replicas. 
You only need to implement your own "state machine," and JPacificA will automatically coordinate between them for you.


## Features
- Consistency guarantee for multiple data replicas
- Fault tolerance: N-1 data replicas are allowed to fail without affecting the overall system availability
- Log replication and replica recovery
- Snapshot and log compression
- Active change Primary
- Partition tolerance in symmetric networks
- Tolerance of asymmetric network partitions

## Requirements
Compilation requires JDK 8 or higher.


## Doc

### How to use
To see [how-to-use](./docs/how-to-use.md)

### About PacificA
To see [《PacificA: Replication in Log-Based Distributed Storage Systems》](./docs/PacificA.pdf)


## Contribution
Contributions to the development of JPacificA are welcome!
If you have any suggestions, questions, or want to contribute code, see our [how-to-contribution](./docs/how-to-contribute.md)

## License
JPacificA is released as open source under the Apache License 2.0. See the [LICENSE](./LICENSE) agreement file for details.
JPacificA relies on a number of third-party components that are also available under the Apache License 2.0.



## Contact way
If you have any questions or suggestions, please contact us at:

- email：cheng.fengfeng@trs.com.cn
- submit issue


## Acknowledgments
JPacificA on architecture and code design, a large number of reference for the design of [SOFAJRaft](https://github.com/sofastack/sofa-jraft),
Thanks to the SOFAJRaft team and all the open source contributors for their hard work and contributions.