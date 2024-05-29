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

JPacificA 是一个基于PacificA一致性算法的生产级高性能日志复制框架的 Java 实现。

## 功能特性
- 多数据副本一致性保证
- 容错性：允许N-1个数据副本故障，不影响系统整体可用性
- 故障副本手动恢复后自动同步数据并恢复一致性
- 日志复制和副本恢复
- 快照和日志压缩
- 主动变更 Primary
- 对称网络分区容忍性
- 非对称网络分区容忍性
- 内置了基于 [Metrics](https://metrics.dropwizard.io/4.0.0/getting-started.html) 类库的性能指标统计，有丰富的性能统计指标
- 通过了 [Jepsen](https://github.com/jepsen-io/jepsen) 一致性验证测试


## 需要
编译需要 JDK 11 及以上。

## 文档

## 贡献
欢迎对 JPacificA 的开发进行贡献！如果你有任何建议、问题或想贡献代码，请参阅我们的 贡献指南。

## 许可证
JPacificA 采用 Apache 许可证 2.0 进行开源发布。有关详细信息，请参阅 [LICENSE](./LICENSE) 协议文件。

## 联系我们
如果你有任何问题或建议，请通过以下方式联系我们：

- 邮件：
- 提交 issue


## 致谢
