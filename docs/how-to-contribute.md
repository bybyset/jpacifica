# JPacificA贡献指南

## 准备工作
贡献代码前需要先了解git工具的使用和GitHub网站的使用。  

## 获取源码
### fork
打开浏览器登录GitHub并找到[JPacifica仓库](https://github.com/JavaCoderCff/jpacifica)，点击左上角的 fork 按钮，复制一份 JPacificA 主干代码到您的代码仓库。

### clone
执行以下命令将代码克隆到本地  
`git clone https://github.com/您的账号名/jpacifica`  

### 分支
执行以下命令后，您的代码仓库就切换到相应分支了  
``git branch add_xxx_feature``  
如果您想切换回主干，执行下面命令  
``git checkout -b master``  
如果您想切换回分支，执行下面命令  
``git checkout -b "branchName"``  

## 修改代码提交到本地
注意事项：
- 代码风格保持一致
- 补充单元测试代码
- 新有修改应该通过已有的单元测试
- 对逻辑和功能不容易被理解的地方添加注释,对于无用的注释，请直接删除
- 应该提供新的单元测试来证明以前的代码存在 bug，而新的代码已经解决了这些 bug

修改完代码后，请按照如下格式执行命令提交所有的修改到本地:

``git commit -am '(feat) 添加xx功能'``  
``git commit -am '(fix) 修复xx功能'``

## 提交代码到远程仓库
在代码提交到本地后，接下来就可以与远程仓库同步代码了。执行如下命令提交本地修改到 Github 上:  
``git push origin "branchname"``

## 提交合并代码到主干的请求
在的代码提交到 GitHub 后，您就可以发送请求来把您改好的代码合入 JPacificA 主干代码了。
此时您需要进入您在 GitHub 上的对应仓库，按右上角的 pull request 按钮。选择目标分支，一般就是 master，
系统会通知 JPacificA 的人员， JPacificA 人员会 Review 您的代码，符合要求后就会合入主干，成为 JPacificA 的一部分。

## 代码 Review
在您提交代码后，您的代码会被指派给维护人员 Review，请耐心等待。
如果在数天后，仍然没有人对您的提交给予任何回复，可以在 PR 下面留言，并 @ 对应的人员。
对于代码 Review 的意见会直接备注到到对应 PR 或者 Issue。如果觉得建议是合理的，也请您把这些建议更新到您的补丁中。

## 合并代码到主干
在代码 Review 通过后，就由 JPacificA 维护人员操作合入主干了。这一步不用参与，代码合并之后，您会收到合并成功的提示。
