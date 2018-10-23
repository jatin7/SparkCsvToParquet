import com.conversion.CsvToParquet;
import com.db.Database;
import com.db.H2Database;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.util.Arrays;

public class DataMigrationApplication {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {


        Database h2Database=new H2Database();
        h2Database.loadRecords();

        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().appName("Spark CSV to Parquet").master("local[*]").getOrCreate();


        String baseDirectoryPath = "/Users/d4109611/project/software/data/sample_data/";
        String[] tableNames = new String[]{"orders"};
        CsvToParquet csvToParquet = new CsvToParquet();

        Arrays.stream(tableNames).forEach(tableName -> {
            try {
                csvToParquet.convert(sparkSession,800, tableName, h2Database, "20170622", "20170623", baseDirectoryPath + "/steelwheels_mirror/", baseDirectoryPath + "/steelwheels_raw/");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });

        sparkSession.stop();
    }
}
