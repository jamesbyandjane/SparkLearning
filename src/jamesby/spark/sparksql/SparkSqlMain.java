package jamesby.spark.sparksql;

import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;


public class SparkSqlMain {
	private static Logger logger = Logger.getLogger(SparkSqlMain.class);
	public static void main(String[] args) throws AnalysisException {
		if("SparkDataFrameTask".equals(args[0])) {
			new SparkDataFrameTask().compute();
		}
		if ("SparkDataSetTask.compute".equals(args[0])) {
			new SparkDataSetTask().compute();
		}
		if ("SparkDataSetTask.rddTransToDataFrame".equals(args[0])) {
			new SparkDataSetTask().rddTransToDataFrame();
		}	
		if ("SparkDataSetTask.testSchema".equals(args[0])) {
			new SparkDataSetTask().testSchema();
		}
		if ("SparkAggregationTask".equals(args[0])) {
			new SparkAggregationTask().compute();
		}
		if ("SparkAggregationTask.test".equals(args[0])) {
			new SparkAggregationTask().test();
		}		
	}
}
