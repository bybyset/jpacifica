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

JPacificA 是一个基于一致性算法PacificA并采用Java语言实现的生产级高性能日志复制框架。
不同与其他“多数节点”的算法如paxos、raft、zab等，它采用的Quorum机制(W+R>N)是N+1>N，
即对于写请求要求“全部节点”，而读请求仅仅需要其中的一个节点，这对于要求“读”性能的场景是非常友好的。
JPacificA是一个通用的框架，你可以基于它在你的应用中实现多个数据副本，以满足“读”服务的高可用，
以及多个数据副本的一致性。你仅仅需要实现自己的“状态机”，JPacificA会自动帮你完成“状态机”之间的一致。

## 功能特性
- 多数据副本一致性保证
- 容错性：允许N-1个数据副本故障，不影响系统整体可用性
- 日志复制和副本恢复
- 快照和日志压缩
- 主动变更 Primary
- 对称网络分区容忍性
- 非对称网络分区容忍性


## 需要
编译需要 JDK 8 及以上。

## 文档

### 如何使用
请参阅文档[《如何使用》](./docs/how-to-use.md)

### 关于PacificA
请参阅原文[《PacificA: Replication in Log-Based Distributed Storage Systems》](./docs/PacificA.pdf)


## 贡献
欢迎对 JPacificA 的开发进行贡献！
如果你有任何建议、问题或想贡献代码，请参阅我们的[《贡献指南》](./docs/how-to-contribute.md)。


## 许可证
JPacificA 采用 Apache License 2.0 进行开源发布。有关详细信息，请参阅 [LICENSE](./LICENSE) 协议文件。
JPacificA 依赖了一些第三方的组件，它们的开源协议也是Apache License 2.0。



## 联系我们
如果你有任何问题或建议，请通过以下方式联系我们：

- 邮件：cheng.fengfeng@trs.com.cn
- 提交 issue


## 致谢
JPacificA在架构和代码设计上，大量参考了[SOFAJRaft](https://github.com/sofastack/sofa-jraft)的设计,
这里感谢SOFAJRaft团队以及所有的开源贡献者，感谢他们的付出和贡献。