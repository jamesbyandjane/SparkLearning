package jamesby.spark.datasource;

import org.apache.spark.sql.SparkSession;
public class SparkSessionUtils implements java.io.Serializable{

	private static final long serialVersionUID = 1L;
	
	public static SparkSession getSparkSession() {
		SparkSession sparkSession = SparkSession
				  .builder()
				  .appName(Constants._APPNAME)
				  .config("master",Constants._MASTER)
				  .getOrCreate();	
		return sparkSession;
	}
	public static void closeSparkSession(SparkSession session) {
		try {
			if (session!=null) {
				session.close();
			}
		}catch(Exception e) {
			
		}
	}
}
