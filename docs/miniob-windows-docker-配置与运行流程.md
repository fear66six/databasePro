# MiniOB 环境配置与运行流程（Windows + Docker + VSCode）

> 本文档仅描述**配置与运行流程**，不包含 Git、Docker、MySQL 客户端的安装步骤。  
> 适用：Windows 本机编辑代码，Docker 提供 Linux 编译/运行环境，通过端口映射在本机连接 miniob。

---

## 一、整体思路

| 组件 | 作用 |
|------|------|
| **Docker** | 提供 Linux 环境，用于在容器内编译和运行 observer |
| **VSCode** | 编辑 miniob 源码；通过 Docker 插件 Attach Shell 进入容器终端 |
| **observer** | miniob 数据库服务端，在容器内运行，监听 6789 端口 |
| **obclient / MySQL 客户端** | 连接工具，向 observer 发送 SQL 并显示结果 |

**目录约定**：假设本机代码目录为 `D:\database\miniob`（可按实际修改）。

---

## 二、配置流程

### 1. 获取 miniob 源码

在本地准备一个目录（如 `D:\database`），在终端执行：

```bash
cd D:\database
git clone https://github.com/oceanbase/miniob.git
```

得到 `D:\database\miniob` 目录。

---

### 2. 创建并启动 Docker 容器（映射代码 + 端口）

在**包含 miniob 的上一层目录**打开终端（如 `D:\database`），执行：

```powershell
cd D:\database

docker run -d --name miniob-dev --privileged `
  -p 6789:6789 `
  -v ${PWD}\miniob:/root/miniob `
  oceanbase/miniob
```

- `-p 6789:6789`：容器 6789 映射到本机，便于本机客户端连接。
- `-v ${PWD}\miniob:/root/miniob`：本机 `miniob` 目录挂载到容器 `/root/miniob`，改代码即改容器内代码。

若本地没有镜像，Docker 会先拉取 `oceanbase/miniob`；若拉取超时，可换源后再执行上述命令。

**之后每次重启电脑**，只需启动已有容器：

```powershell
docker start miniob-dev
```

---

### 3. 进入容器并编译

**方式 A：VSCode Docker 插件**

- 打开 VSCode，左侧 Docker 插件中找到容器 `miniob-dev`。
- 右键 → **Attach Shell**，在打开的终端中即为容器内 shell。

**方式 B：本机终端**

```powershell
docker exec -it miniob-dev bash
```

在容器内执行：

```bash
cd /root/miniob
bash build.sh
```

等待编译完成。成功后会有 `build`（指向 `build_debug`）及 `build_debug` 目录，可执行文件在 `build/bin/` 下。

---

### 4. 启动 observer（数据库服务端）

**必须在 `/root/miniob` 下执行**，否则会报 `No such file or directory`。

在容器内：

```bash
cd /root/miniob
./build/bin/observer -f ./etc/observer.ini -p 6789
```

看到类似输出即表示启动成功：

```
Successfully load ./etc/observer.ini
```

保持该终端不关闭，observer 会一直监听 6789 端口。

---

### 5. 连接 miniob（客户端）

需要**另开一个终端**（再 Attach Shell 一次或再开一个 `docker exec`）。

**在容器内使用 obclient：**

```bash
cd /root/miniob
./build/bin/obclient -p 6789
```

出现 `miniob >` 即表示已连接，可输入 SQL。

**在本机 Windows 使用 MySQL 客户端：**

确保本机已安装 MySQL 命令行或图形客户端，且容器已映射 `-p 6789:6789`：

```bash
mysql -h 127.0.0.1 -P 6789 -u root
```

图形客户端（HeidiSQL、DBeaver、DataGrip 等）：新建连接，主机 `127.0.0.1`，端口 `6789`，用户 `root`，无密码可留空。

---

### 6. 做实验时的基本用法

在 `miniob >` 或 MySQL 客户端中执行 SQL，例如：

```sql
create table t1(id int, name char(20));
insert into t1 values(1, 'a');
insert into t1 values(2, 'b');
select * from t1;
desc t1;
```

退出 obclient：输入 `exit` 或 `quit`。  
停止 observer：在运行 observer 的终端按 `Ctrl+C`。

---

## 三、常见问题

| 现象 | 原因 | 处理 |
|------|------|------|
| `./bin/observer: No such file or directory` | 当前目录不对（如在 `/root` 下执行） | 先 `cd /root/miniob`，再执行 `./build/bin/observer ...` |
| `cd build_debug` 报 No such file or directory | 尚未编译或目录名不同 | 在 `/root/miniob` 下执行 `bash build.sh`，用 `./build/bin/observer`（build 会链接到 build_debug） |
| 本机 mysql 连不上 127.0.0.1:6789 | 容器未做端口映射或 observer 未启动 | 确保 `docker run` 时有 `-p 6789:6789`，且容器内 observer 已启动 |

---

## 四、流程速览

1. 本机 clone miniob → 得到 `D:\database\miniob`。
2. 在 `D:\database` 下执行 `docker run ... -p 6789:6789 -v ... miniob`，创建并启动容器。
3. Attach Shell 或 `docker exec` 进入容器 → `cd /root/miniob` → `bash build.sh`。
4. 在 `/root/miniob` 下执行 `./build/bin/observer -f ./etc/observer.ini -p 6789`，保持运行。
5. 新开终端，`./build/bin/obclient -p 6789`（容器内）或本机 `mysql -h 127.0.0.1 -P 6789 -u root`。
6. 在客户端输入 SQL 完成实验；改代码后重新 `bash build.sh` 并重启 observer 再测。

---

*文档根据实际配置与运行过程整理，便于复现与查阅。*
