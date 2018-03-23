package io.saagie.demos.multihive;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;

public class DataCopier {

    /**
     * Main.
     *
     * @param args: arguments ("source thrift server" "destination thrift server" "source query" "destination table")
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String srcThriftServer = args[0];
        String dstThriftServer = args[1];
        String srcQuery = args[2];
        String dstTable = args[3];

        SparkSession srcSession = SparkSession.builder().master("local")
                .config("hive.metastore.uris", "thrift://" + srcThriftServer)
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> srcData = srcSession.sql(srcQuery);
        //StructType srcSchema = srcData.schema();


        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();


        SparkSession dstSession = SparkSession.builder().master("local")
                .config("hive.metastore.uris", "thrift://" + dstThriftServer)
                .config("spark.sql.crossJoin.enabled", true)
                .enableHiveSupport()
                .getOrCreate();

        dstSession.sql("drop table if exists " + dstTable);

        /*ArrayList<String> joinColList = new ArrayList<String>();
        joinColList.add("name");

        Dataset<Row> dstData = dstSession.createDataFrame(Collections.<Row>emptyList(), srcSchema)   //.emptyDataFrame()
                .join(srcData, scala.collection.JavaConversions.asScalaBuffer(joinColList), "right_outer"); //.crossJoin(srcData);*/

        Dataset<Row> dstData = dstSession.emptyDataFrame()
                .join(srcData, scala.collection.JavaConversions.asScalaBuffer(Collections.<String>emptyList()), "right_outer");

        //dstData.explain();

        dstData.show(10);

        dstData.write().saveAsTable(dstTable);

        //dstData.createOrReplaceTempView("tmp_" + ts);

        //dstSession.sql("create table " + dstTable + " as select * from tmp_" + ts);

        //Dataset<Row> testData = dstSession.sql("select * from " + dstTable);
        //testData.printSchema();
        //System.out.println(testData.count());
    }
}
