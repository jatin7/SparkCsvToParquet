package com.conversion;

import com.db.Database;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class CsvToParquet {


    public String convert(SparkSession sparkSession, int noOfPartion, String tableName, Database db, String lastSnapshotDate, String thisSnapshotDate, String outputDirectoryPath, String rawInputDirectoryPath) throws ClassNotFoundException {


        Long min = 1L; //use table stats or similar to profile the table to get these stats dynamically for the table being synced and will result in data spread over number of partitions in connection properties
        Long max = 1000000L;

        String deltaPath = rawInputDirectoryPath + "delta/" + tableName + "/d";
        String snapshotPath = rawInputDirectoryPath + "snapshot/" + tableName + "/d";
        String mirrorPath = outputDirectoryPath + "merged/" + tableName + "/d";

        writeToOrcFormat(sparkSession, lastSnapshotDate, thisSnapshotDate,deltaPath, snapshotPath);
        // This is additional write so that we can check LastSnapshot is written then how system behave


        Dataset<Row> dsLastSnapshot = sparkSession.read().orc(snapshotPath + lastSnapshotDate + "/orc");
        String[] snapshotColumns = createSnapshotDataView(tableName, dsLastSnapshot);
        String snapshotColumnsString = String.join(",", snapshotColumns);

        String deltaCriteria = "where dte1 > sysdate -10 or dte2 > sysdate -10 etc...";

        if(dsLastSnapshot.count()==0){
            deltaCriteria="";
        }
        String[] deltaColumns = creteDeltaDataView(sparkSession, tableName,"", min,max,noOfPartion,db, deltaCriteria);
        String deltaColumnsString = String.join(",", deltaColumns);

        if(snapshotColumns.length!=deltaColumns.length){
            throw new RuntimeException("Columns between Snapshot and Delta Tables does not match");
        }

        Dataset<Row> dsMerged = mergeData(sparkSession, tableName, snapshotColumnsString, deltaColumnsString);
        dsMerged.printSchema();

        Dataset<Row> dsFinal = renameColumnMergedToFinal(snapshotColumns, deltaColumns, dsMerged);
        dsFinal.printSchema();

        String finalOutputPath = mirrorPath + thisSnapshotDate + "/parquet";
        dsFinal.write().mode(SaveMode.Overwrite).parquet(finalOutputPath);

        return finalOutputPath;
    }

    private Dataset<Row> renameColumnMergedToFinal(String[] snapshotColumns, String[] deltaColumns, Dataset<Row> dsMerged) {
        Map<String, String> columnNames = IntStream.range(0, deltaColumns.length).boxed().collect(Collectors.toMap(Arrays.asList(snapshotColumns)::get, Arrays.asList(deltaColumns)::get));
        List<String> newColumns = columnNames.keySet().stream().map(s -> s.toLowerCase() + " as " + columnNames.get(s).toLowerCase())
                .collect(Collectors.toList());
        return dsMerged.selectExpr(JavaConverters.asScalaIteratorConverter(newColumns.iterator()).asScala().toSeq());
    }   

    private Dataset<Row> mergeData(SparkSession sparkSession, String tableName, String snapshotColumsString, String deltaColumsString) {
        String mergequery = "select  " + snapshotColumsString + " from(" +
                " select *,dense_rank() OVER(PARTITION BY " + snapshotColumsString + " ORDER BY index DESC) as rank from (" +
                " select " + snapshotColumsString + ", 0 as index from snapshot_" + tableName +
                " union all" +
                " select " + deltaColumsString + ", 1 as index from delta_" + tableName + ")) where rank =1 " +
                " ";

        Dataset<Row> dsMerged = sparkSession.sql(mergequery);
        dsMerged.printSchema();
        return dsMerged;
    }

    private String[] creteDeltaDataView(SparkSession sparkSession, String tableName, String columns, long min, long max, int noOfPartion, Database db, String deltaCriteria) throws ClassNotFoundException {
        Class.forName("org.h2.Driver"); //make sure oracle driver is on the server and included in class path

        Dataset<Row> dsDelta = sparkSession.read().jdbc(db.getDBUrl(), tableName+" "+ deltaCriteria, columns,min,max,noOfPartion,db.getConnectionProperties());
        //Dataset<Row> dsDelta = sparkSession.read().jdbc(db.getDBUrl(), tableName, db.getConnectionProperties()).distinct().cache();
        // Dataset<Row> dsDelta = sparkSession.read().jdbc(jdbcUrl, "select * from " + schemaName + "." + tableName + " " + deltaCriteria + "," + primaryKeys, conPro).distinct().cache();
        //pk's are used automatically to partition and parrallelalise the read
        dsDelta.createOrReplaceTempView("delta_" + tableName);
        dsDelta.printSchema();
        return dsDelta.columns();
    }

    private String[] createSnapshotDataView(String tableName, Dataset<Row> dsLastSnapshot) {
        dsLastSnapshot.createOrReplaceTempView("snapshot_" + tableName);
        dsLastSnapshot.printSchema();
        return dsLastSnapshot.columns();
    }

    private static void writeToOrcFormat(SparkSession sparkSession,String lastSnapshotDate, String thisSnapshotDate, String deltaPath, String snapshotPath) {
        sparkSession.read().csv(deltaPath + lastSnapshotDate + "/data.csv").write().mode(SaveMode.Overwrite).orc(snapshotPath + lastSnapshotDate + "/orc");
    }

}
