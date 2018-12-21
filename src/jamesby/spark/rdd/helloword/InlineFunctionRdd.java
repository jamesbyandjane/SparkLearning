package jamesby.spark.rdd.helloword;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class InlineFunctionRdd implements java.io.Serializable{
	private static final long serialVersionUID = 1L;

	protected int triggerCompute() {
		JavaSparkContext sc = getJavaSparkContext();
		int ret = compute(sc);
		sc.close();
		return ret;
	}
	
	private JavaSparkContext getJavaSparkContext(){
		SparkConf conf = new SparkConf().setAppName(getAppName()).setMaster(Constants._MASTER);
		JavaSparkContext sc = new JavaSparkContext(conf);	
		return sc;
	}	
	
	public String getAppName() {
		return "InlineFunctionRdd";
	}
	
	public int compute(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(Constants.filename01);
		
		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(String s) { 
				return s.length(); 
			}
		});
		
		int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer a, Integer b) { 
				return a + b; 
			}
		});

		return totalLength;
	}
	
}
