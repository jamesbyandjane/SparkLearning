package jamesby.spark.sparksql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.esotericsoftware.minlog.Log;

import jamesby.spark.utils.SparkConstants;
import jamesby.spark.utils.SparkSessionUtils;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Logger;

public class SparkDataFrameTask {
	private static Logger logger = Logger.getLogger(SparkDataFrameTask.class);
	public void compute() throws AnalysisException {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			Dataset<Row> df = session.read().json(SparkConstants._FILE_NAME);
		
			Log.info("**********BEGIN:printSchema*************");
			df.printSchema();
			Log.info("**********END:printSchema*************");
			
			Log.info("**********BEGIN:show*************");
			df.select("name").show();
			Log.info("**********END:show*************");
			
			Log.info("**********BEGIN:select*************");
			df.select(col("name"), col("review_count").plus(1)).show();
			Log.info("**********END:select*************");
			
			Log.info("**********BEGIN:filter*************");
			df.filter(col("review_count").gt(5)).show();
			Log.info("**********END:filter*************");
			
			Log.info("**********BEGIN:groupBy*************");
			df.groupBy("name").count().show();
			Log.info("**********END:groupBy*************");
			/**
			 * 临时View是同Session相关的，仅仅当前Session有效。
			 */
			Log.info("**********BEGIN:createOrReplaceTempView*************");
			df.createOrReplaceTempView("people");
			Dataset<Row> sqlDF = session.sql("SELECT * FROM people");
			sqlDF.show();
			Log.info("**********END:createOrReplaceTempView*************");
			
			/**
			 * 全局View
			 */
			Log.info("**********BEGIN:createGlobalTempView.1*************");
			df.createGlobalTempView("people");
			session.sql("SELECT * FROM global_temp.people").show();
			Log.info("**********END:createGlobalTempView.1*************");
			
			Log.info("**********BEGIN:createGlobalTempView.2*************");
			session.newSession().sql("SELECT * FROM global_temp.people").show();
			Log.info("**********END:createGlobalTempView.2*************");
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
}
