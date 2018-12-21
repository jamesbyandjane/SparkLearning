package jamesby.spark.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DatasourceTask {
	public void launch() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			loadJsonToParquet(session);
			loadParquetToParquet(session);
			loadParquetToCsv(session);
			loadCsvToOrc(session);
			loadParquetWithSql(session);
			//loadParquetToBucket(session);			
			loadParquetToPartition(session);
			loadParquetToPartitionAndBucket(session);
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
	public void loadJsonToParquet(SparkSession session) {
		Dataset<Row> peopleDF = session.read().format("json").load(Constants.filename03);
		peopleDF.select("name", "salary").write().mode(SaveMode.Overwrite).format("parquet").save(Constants._FILE_PATH + "/NameAndSalary.parquet");
	}
	
	public void loadParquetToParquet(SparkSession session) {
		Dataset<Row> usersDF = session.read().load(Constants._FILE_PATH + "/NameAndSalary.parquet");
		usersDF.select("name", "salary").write().mode(SaveMode.Overwrite).save(Constants._FILE_PATH + "/NameAndSalary2.parquet");
	}
	
	public void loadParquetToCsv(SparkSession session) {
		Dataset<Row> usersDF = session.read().load(Constants._FILE_PATH + "/NameAndSalary.parquet");
		usersDF.select("name","salary").write().mode(SaveMode.Overwrite).format("csv")
			.option("sep", ";")
			.option("inferSchema", "true")
			.option("header", "true")
			.save(Constants._FILE_PATH + "/Employee.csv");
	}
	
	public void loadCsvToOrc(SparkSession session) {
		Dataset<Row> peopleDFCsv = session.read().format("csv")
				  .option("sep", ";")
				  .option("inferSchema", "true")
				  .option("header", "true")
				  .load(Constants._FILE_PATH + "/Employee.csv");			
			
		peopleDFCsv.write().mode(SaveMode.Overwrite).format("orc")
		  .option("orc.bloom.filter.columns", "favorite_color")
		  .option("orc.dictionary.key.threshold", "1.0")
		  .save(Constants._FILE_PATH + "/Employee.orc");
	}
	
	public void loadParquetWithSql(SparkSession session) {
		Dataset<Row> sqlDF =session.sql("SELECT * FROM parquet.`"+Constants._FILE_PATH + "/NameAndSalary.parquet"+"`");			sqlDF.show();
	}	
	
	public void loadParquetToBucket(SparkSession session) {
		Dataset<Row> usersDF = session.read().load(Constants._FILE_PATH + "/NameAndSalary.parquet");
		usersDF.write().bucketBy(42, "name").sortBy("salary").mode(SaveMode.Overwrite).saveAsTable("employee_bucketed");
	}
	
	public void loadParquetToPartition(SparkSession session) {
		Dataset<Row> usersDF = session.read().load(Constants._FILE_PATH + "/NameAndSalary.parquet");
		usersDF
		  .write()
		  .partitionBy("salary")
		  .format("parquet")
		  .mode(SaveMode.Overwrite)
		  .save(Constants._FILE_PATH + "/NameAndSalary_Partition.parquet");		
	}
	
	public void loadParquetToPartitionAndBucket(SparkSession session) {
		Dataset<Row> usersDF = session.read().load(Constants._FILE_PATH + "/NameAndSalary.parquet");
		usersDF
		  .write()
		  .partitionBy("salary")
		  .bucketBy(42, "name")
		  .saveAsTable("employee_partitioned_bucketed");
	}
}
