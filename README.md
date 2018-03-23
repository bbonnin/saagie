# Saagie

Code examples

* R
  * test_jdbc.R: example of R code for querying a JDBC-compliant database, such as Hive (see also `rjdbc-hive` project below for the needed jars)
  * read_write_hdfs.R: example for reading and writing files in HDFS

* python
  * read_write_hdfs.py: example for reading and writing files in HDFS
  * test_hive.py: Hive queries

* distcp
  * readme about how to copy files between clusters

* hive
  * multi-hive: java/spark project to copy data in Hive from one cluster to another 

* utils
  * jdbc-hive: jar with Hive JDBC driver and all the dependencies (can be used with RJDBC)
    * The jar is available in the `target` directory
  * simple-proxy      

> Yes, it's a shame to put jar files in a git repository, I know...
