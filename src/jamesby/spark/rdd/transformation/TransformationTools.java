package jamesby.spark.rdd.transformation;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import jamesby.spark.rdd.helloword.Constants;

public class TransformationTools {
	private static Logger logger = Logger.getLogger(TransformationTools.class);
	public static JavaSparkContext getSparkContext() {
		SparkConf conf = new SparkConf().setAppName(TransformationConstants._APPNAME).setMaster(Constants._MASTER);
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
