# UDFs

A collection of User Defined Functions for hive.

## Quickstart 
```
$ git clone https://github.com/SimonJPegg/UDFs.git
$ cd UDFs
$ mvn clean package
```

## Install
### add the following to hive-site.xml
```
<property>
	<name>hive.aux.jars.path</name>
	<value>hdfs://path/to/udfs-<version>.jar</value>
</property>
```

### Register the udfs
```
hive>  add jar /path/to/udfs-<version>.jar
hive> CREATE FUNCTION clean_str AS 'org.antipathy.udfs.CleanStr'
hive> CREATE FUNCTION last_day AS 'org.antipathy.udfs.LastDayOfMonth'
hive> CREATE FUNCTION str_to_timestamp AS 'org.antipathy.udfs.StrToTimeStamp'
```


