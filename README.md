[![License](https://img.shields.io/badge/License-GPL3-blue.svg)](http://www.gnu.org/licenses/gpl-3.0.html)
[![Python versions](https://img.shields.io/badge/Python-2.7.10-blue.svg)](https://www.python.org)


[TOC]: # "目录"

# 目录
- [项目描述](#项目描述)
- [安装](#安装)
    - [JimV-N 快速安装](#jimv-n-快速安装)
    - [[JimV-N 手动安装](docs/install.md)](#jimv-n-手动安装)
- [问题反馈](#问题反馈)
- [项目成员](#项目成员)


## 项目描述

> JimV 的计算节点。


## 安装

### JimV-N 快速安装
> 在一台服务器上仅部署 JimV-N。使其成为 JimV 虚拟化环境的计算节点。

1. [安装、初始化 JimV-C](https://github.com/jamesiter/JimV-C#%E5%AE%89%E8%A3%85)
2. 安装 JimV-N
    ``` bash
    # 避免各种意外的 ssh 断开。如果遇到因网络问题而断开的意外，那么再次连接后，使用 screen -r 可以恢复到断开前的终端环境。
    yum install screen -y
    echo 'termcapinfo xterm* ti@:te@' > ~/.screenrc
    screen
    curl https://raw.githubusercontent.com/jamesiter/JimV-N/master/INSTALL.sh | bash -s -- --redis_host {x.x.x.x} --redis_password {password} --redis_port {port}
    ```
3. 启动 JimV-N
    ``` bash
    cd /opt/JimV-N && ./startup.sh
    ```


### [JimV-N 手动安装](docs/install.md)


## 问题反馈

[提交Bug](https://github.com/jamesiter/JimV-N/issues) <br> 技术交流 QQ 群:
377907881


## 项目成员

<pre>
姓名:    James Iter
E-Mail: james.iter.cn@gmail.com
</pre>

