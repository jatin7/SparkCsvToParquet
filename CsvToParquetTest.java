import com.conversion.CsvToParquet;
import com.db.H2Database;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;


@RunWith(JUnit4.class)
public class CsvToParquetTest   {

    SparkSession sparkSession;

    @Before
    public void setup(){
        sparkSession= SparkSession.builder().master("local").getOrCreate();//Mockito.mock(SparkSession.class);
        //Mockito.when(sparkSession.re).

    }

    @After
    public void tearDown(){
        sparkSession.stop();
    }
    @Test
    public void testCsvToConversation() throws ClassNotFoundException, IOException {

        CsvToParquet csvToParquet = new CsvToParquet();
        String baseDirectoryPath = "/Users/d4109611/project/software/data/sample_data/";
        baseDirectoryPath="src/test/resources/sample_data/";
        String outputDirectoryPath = baseDirectoryPath + "/steelwheels_mirror/";
        String rawInputDirectoryPath = baseDirectoryPath + "/steelwheels_raw/";
        String tableName = "orders";
        String thisSnapshotDate = "20170623";
        String tableOutputPath=outputDirectoryPath+"merged/" + tableName + "/d";
        String tableParquetOutputPath = tableOutputPath + thisSnapshotDate ;
        FileUtils.deleteDirectory(new File(tableParquetOutputPath));


        Long outputRecords = countOutputRecords(sparkSession, tableName, tableParquetOutputPath);
        Assert.assertTrue(0== outputRecords);

        String outputPath = csvToParquet.convert(sparkSession, 800,
                tableName, new H2Database(), "20170622", thisSnapshotDate, outputDirectoryPath, rawInputDirectoryPath);

        outputRecords = countOutputRecords(sparkSession, tableName, outputPath);
        System.out.println("Record Count->"+outputRecords);
        Assert.assertTrue(0< outputRecords);
    }

    private Long countOutputRecords(SparkSession sparkSession, String tableName, String tableOutputPath) {
        try {
            return sparkSession.read().parquet(tableOutputPath).count();
        }catch (Exception e){
            e.printStackTrace();
            return 0L;
        }
    }
}
