package jamesby.spark.rdd.broadcast;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import jamesby.spark.rdd.helloword.Constants;

public class BroadcastTools {
	private static Logger logger = Logger.getLogger(BroadcastTools.class);
	public static JavaSparkContext getSparkContext() {
		SparkConf conf = new SparkConf().setAppName(BroadcastConstants._APPNAME).setMaster(Constants._MASTER);
		JavaSparkContext sc = new JavaSparkContext(conf);	
		return sc;		
	}
	
	public static void close(JavaSparkContext sc) {
		try {
			if (sc!=null)
				sc.close();
		}catch(Exception e) {
			logger.error(e);
		}
	}
}
