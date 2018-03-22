# distcp

Details about `distcp`: https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html

> Important: 
> * distcp must have read access on the source HDFS and write access on the destination HDFS
> * the user used to run distcp is the user used to access both cluster !

## Examples

* Copy a directory
```sh
$ hadoop distcp hdfs://cluster_source/home/someone/mydirectory hdfs://cluster_destination/tmp
```

* Copy multiple files
```sh
# the file `files_to_copy` contains the URI of files
$ hdfs dfs -cat hdfs://cluster_source/home/someone/files_to_copy
hdfs://cluster_source/home/someone/file1
hdfs://cluster_source/home/someone/file2
hdfs://cluster_source/home/someone/file3

$ hadoop distcp -f hdfs://cluster_source/home/someone/files_to_copy hdfs://cluster_destination/tmp
```

