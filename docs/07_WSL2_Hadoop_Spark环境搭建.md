# WSL2 Hadoop Spark 环境搭建

## 1. 目标

本文档用于在 Windows 的 WSL2 Ubuntu 环境中搭建课程项目所需的大数据运行环境：

```text
WSL2 Ubuntu
  |
  +-- Java
  +-- Hadoop HDFS 单节点伪分布式
  +-- Spark / PySpark
  +-- 当前项目目录 /mnt/g/ProjectComplex/Dashuju
```

项目代码仍然放在 Windows 的 `G:\ProjectComplex\Dashuju`，在 WSL 中通过以下路径访问：

```bash
/mnt/g/ProjectComplex/Dashuju
```

## 2. 检查 WSL 环境

在 WSL Ubuntu 终端中执行：

```bash
cd /mnt/g/ProjectComplex/Dashuju
pwd
ls
```

确认能看到项目文件：

```text
README.md
config
docs
scripts
spark_jobs
```

## 3. 安装基础依赖

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk ssh rsync wget curl python3 python3-pip
```

检查 Java：

```bash
java -version
```

查找 Java 安装路径：

```bash
readlink -f $(which java)
```

常见 `JAVA_HOME`：

```text
/usr/lib/jvm/java-11-openjdk-amd64
```

## 4. 配置 SSH 免密

Hadoop 伪分布式启动 DataNode 时需要本机 SSH。

```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

启动 SSH 服务：

```bash
sudo service ssh start
```

验证：

```bash
ssh localhost
```

第一次输入 `yes`，能登录后输入：

```bash
exit
```

## 5. 安装 Hadoop

下载并解压：

```bash
cd ~
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
tar -xzf hadoop-3.4.2.tar.gz
sudo mv hadoop-3.4.2 /usr/local/hadoop
```

如果下载地址不可用，可以到 Apache Hadoop 官网选择当前稳定版本，并保持后续路径为：

```text
/usr/local/hadoop
```

## 6. 配置环境变量

编辑 `~/.bashrc`：

```bash
nano ~/.bashrc
```

追加：

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

使配置生效：

```bash
source ~/.bashrc
```

检查 Hadoop：

```bash
hadoop version
hdfs version
```

## 7. 配置 Hadoop 伪分布式

编辑 Hadoop 配置目录：

```bash
cd $HADOOP_HOME/etc/hadoop
```

### 7.1 `hadoop-env.sh`

```bash
nano hadoop-env.sh
```

设置：

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### 7.2 `core-site.xml`

```bash
nano core-site.xml
```

内容：

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/usr/local/hadoop/tmp</value>
  </property>
</configuration>
```

### 7.3 `hdfs-site.xml`

```bash
nano hdfs-site.xml
```

内容：

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/usr/local/hadoop/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/usr/local/hadoop/hdfs/datanode</value>
  </property>
</configuration>
```

### 7.4 `mapred-site.xml`

```bash
cp mapred-site.xml.template mapred-site.xml
nano mapred-site.xml
```

内容：

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

### 7.5 `yarn-site.xml`

```bash
nano yarn-site.xml
```

内容：

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_HOME,HADOOP_CONF_DIR,PATH</value>
  </property>
</configuration>
```

## 8. 格式化并启动 HDFS

首次使用需要格式化 NameNode：

```bash
hdfs namenode -format
```

启动 HDFS：

```bash
start-dfs.sh
```

检查进程：

```bash
jps
```

应该能看到类似：

```text
NameNode
DataNode
SecondaryNameNode
```

检查 HDFS：

```bash
hdfs dfs -ls /
```

如果没有报错，HDFS 已可用。

## 9. 安装 Spark

下载并解压：

```bash
cd ~
wget https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
sudo mv spark-3.5.6-bin-hadoop3 /usr/local/spark
```

如果下载地址不可用，可以到 Apache Spark 官网选择当前稳定版本，优先选择 `bin-hadoop3` 包。

编辑 `~/.bashrc`：

```bash
nano ~/.bashrc
```

追加：

```bash
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
```

使配置生效：

```bash
source ~/.bashrc
```

检查 Spark：

```bash
spark-submit --version
pyspark --version
```

## 10. 安装项目 Python 依赖

进入项目目录：

```bash
cd /mnt/g/ProjectComplex/Dashuju
```

建议使用虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

如果不想使用虚拟环境，也可以直接：

```bash
pip3 install -r requirements.txt
```

## 11. 项目验证

### 11.1 生成样例数据

```bash
cd /mnt/g/ProjectComplex/Dashuju
python3 scripts/generate_data.py \
  --matches 20 \
  --players 80 \
  --days 2 \
  --start-date 2026-05-12 \
  --output-dir data/local_raw/game_event_log \
  --overwrite
```

检查输出：

```bash
find data/local_raw/game_event_log -type f
head -n 2 data/local_raw/game_event_log/dt=2026-05-12/events.jsonl
```

### 11.2 初始化 HDFS 目录

先预览：

```bash
bash scripts/init_hdfs_dirs.sh --dry-run
```

真实执行：

```bash
bash scripts/init_hdfs_dirs.sh
```

### 11.3 上传 ODS 原始数据

先预览：

```bash
bash scripts/upload_to_hdfs.sh \
  --local-path data/local_raw/game_event_log \
  --dry-run
```

真实上传：

```bash
bash scripts/upload_to_hdfs.sh \
  --local-path data/local_raw/game_event_log \
  --overwrite
```

检查 HDFS：

```bash
hdfs dfs -ls /game_balance/ods/game_event_log
hdfs dfs -ls /game_balance/ods/game_event_log/dt=2026-05-12
hdfs dfs -cat /game_balance/ods/game_event_log/dt=2026-05-12/events.jsonl | head -n 2
```

## 12. 常见问题

### 12.1 `hdfs: command not found`

说明 Hadoop 环境变量没有生效。

处理：

```bash
source ~/.bashrc
echo $HADOOP_HOME
which hdfs
```

### 12.2 `start-dfs.sh` 无法 SSH localhost

检查 SSH 服务：

```bash
sudo service ssh start
ssh localhost
```

### 12.3 `NameNode is not formatted`

首次启动前执行：

```bash
hdfs namenode -format
```

### 12.4 WSL 关闭后 Hadoop 停了

WSL 关闭后 Hadoop 进程会停止。重新打开 WSL 后执行：

```bash
sudo service ssh start
start-dfs.sh
```

### 12.5 Windows 路径和 WSL 路径对应关系

Windows：

```text
G:\ProjectComplex\Dashuju
```

WSL：

```bash
/mnt/g/ProjectComplex/Dashuju
```

## 13. 当前阶段完成标准

完成以下命令并无报错，即认为环境搭建完成：

```bash
java -version
hdfs version
spark-submit --version
cd /mnt/g/ProjectComplex/Dashuju
bash scripts/init_hdfs_dirs.sh
bash scripts/upload_to_hdfs.sh --local-path data/local_raw/game_event_log --overwrite
hdfs dfs -ls /game_balance/ods/game_event_log
```

