# Multi-Hive

Copy data from a table in one Hive to another one

* Build
```sh
mvn clean package -Ppkg
```

* Deploy:
	* in Saagie's platform, create a new Spark job
	* paramters are: "source thrift server" "destination thrift server" "source query" "destination table"
	* example: 172.148.44.40:9083 172.17.0.2:9083  "select * from churn.customer_details" "default.customers"

