# Oracle Handler for Oracle GoldenGate

The datahub handler processes the operations of OGG trails, and upload the change logs into Oracle. It has the following features:

## Getting Started
---

### Requirements

1. JDK 1.6 or later (JDK 1.7 recommended)
2. Apache Maven 3.x
3. OGG Java Adapter

### Build the Package

Build the package with the script file:

```
$ cd zjepe-oracle-ogg-plugin/
$ ./build.sh
```

Wait until building success. The oracle handler will be in **zjepe-oracle-ogg-plugin/build_1.0.0/oracle_lib/**

### Use Oracle Handler

1. Configure oracle handler for OGG extract.

    Set goldengate writers to `jvm`:

    ```
    goldengate.userexit.writers=jvm
    ```

    Set jvm boot options and goldengate classpath:

    ```
    jvm.bootoptions=-Djava.class.path=ggjava/ggjava.jar -Xmx512m -Xms32m 
    gg.classpath=/.../oracle_lib/*
    ```

    Set the handler name and type:

    ```
    gg.handlerlist=ggoracle
    gg.handler.ggoracle.type=com.zjepe.oracle.ogg.handler.OracleHandler
    ```

2. Configure other handler parameters.

3. Start OGG extract, then the handler will start uploading data into Oracle.

### Handler Parameter Details

Following is a sample properties file of OGG oracle handler:


```
# extoracle.properties

# Set writers name
goldengate.userexit.writerss=jvm

# Set handler name
gg.handlerlist=ggoracle

# Set goldengate classpath
gg.classpath=YOUR_ORACLE_HANDLER_DIRECTORY/oracle_lib/*

# Set jvm boot options
jvm.bootoptions=-Djava.class.path=YOUR_ORACLE_PLUGIN_DIRECTORY/ggjava/ggjava.jar -Xmx512m -Xms32m

# Handler type of ggoracle, need to be exactly the same as below
gg.handler.ggoracle.type=com.zjepe.oracle.ogg.handler.OracleHandler

# JDBC driver name
gg.handler.ggoracle.driverName=oracle.jdbc.driver.OracleDriver

# JDBC url
gg.handler.ggoracle.url=jdbc:oracle:thin:@YOUR_ADDRESS:1521:YOUR_INSTANCE_NAME

# JDBC userName
gg.handler.ggoracle.userName=YOUR_USER_NAME

# JDBC password
gg.handler.ggoracle.password=YOUR_PASSWORD

# ORACLE target table type.Divided into two types:history or final
gg.handler.ggoracle.targetTableType=test_1/history,test_2/final

# ORACLE target deal situation.Divided into two situations:history or final
gg.handler.ggoracle.targetDealSituation=test_1/final,test_2/history

# OGG ORACLE table map
gg.handler.ggoracle.tableMap=test_1/ogg_test1,test_2/ogg_test2

# Specify the field names of the keys in OGG, 数据类型目前支持四种:STRING(代表oracle所有的字符串类型，如：varchar、char等)、NUMBER(代表oracle的所有数字类型)、BLOB(代表非结构化二进制数据大对象)、CLOB(代表字符数据大对象)
gg.handler.ggoracle.keyFields=test_1:columna/STRING,columnb/STRING|test_2:columna/STRING

# Specify the field names of the columns to be watched in OGG, 数据类型目前支持四种:STRING(代表所有oracle所有的字符串类型，如：varchar/char)、NUMBER(代表oracle的所有数字类型)、BLOB(非结构化二进制数据大对象)、CLOB(字符数据大对象)
gg.handler.ggoracle.focusFields=test_1:columnc/NUMBER,columnd/CLOB|test_2:columnb/STRING,columnc/NUMBER,columnd/CLOB

# Specify the maximum retry count of write pack. Defaulting to 3.
gg.handler.ggoracle.retryCount=3

```

### Use Custom Alarm

The Alarm interface have 4 methods with 2 different alarm level, *warn* and *error*. The warn level indicates there might be some problem, but the handler can keep working.
The error level indicates a fatal error. In most cases, the OGG extract will abend when a fatal error occurred.

Following are the methods in the alarm interface:

```
public interface OggAlarm {
    public void warn(String msg);
    public void warn(String msg, Throwable throwable);

    public void error(String msg);
    public void error(String msg, Throwable throwable);
}
```

A default implementation is LogAlarm that records the logs using *log4j*.

## Authors
---

- [Wang Chao]
