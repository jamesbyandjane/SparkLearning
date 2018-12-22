package jamesby.spark.streaming;

import org.apache.log4j.Logger;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkStreamingMain {
	private static Logger logger = Logger.getLogger(SparkStreamingMain.class);
	public static void main(String[] args) throws StreamingQueryException {
		new SparkStreamingTask().compute();
	}
}
