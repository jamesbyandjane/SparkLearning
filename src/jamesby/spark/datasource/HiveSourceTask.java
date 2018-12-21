package jamesby.spark.datasource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveSourceTask {
	public void launch() {
		SparkSession spark = SparkSession
				  .builder()
				  .appName(Constants._APPNAME)
				  .config("master",Constants._MASTER)
				  .enableHiveSupport()
				  .getOrCreate();	
		try {
			spark.sql("CREATE TABLE IF NOT EXISTS src2 (key INT, value STRING) USING hive");
			spark.sql("LOAD DATA LOCAL INPATH '/usr/local/software/spark-2.4.0-bin-hadoop2.7/study/kv1.txt' INTO TABLE src2");

			spark.sql("SELECT * FROM src2").show();
			spark.sql("SELECT COUNT(*) FROM src2").show();

			Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src2 WHERE key < 10 ORDER BY key");

			Dataset<String> stringsDS = sqlDF.map(
			    (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
			    Encoders.STRING());
			stringsDS.show();

			List<Record> records = new ArrayList<>();
			for (int key = 1; key < 100; key++) {
			  Record record = new Record();
			  record.setKey(key);
			  record.setValue("val_" + key);
			  records.add(record);
			}
			Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
			recordsDF.createOrReplaceTempView("records");

			spark.sql("SELECT * FROM records r JOIN src2 s ON r.key = s.key").show();
			
		}finally {
			SparkSessionUtils.closeSparkSession(spark);
		}
	}
	
	public static class Record implements Serializable {
		private static final long serialVersionUID = 1L;
		private int key;
		private String value;

		public int getKey() {
			return key;
		}

		public void setKey(int key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}
}
