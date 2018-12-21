package jamesby.spark.rdd.helloword;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class ExternalFileRdd extends BaseRdd{
	
	public String getAppName() {
		return "ExternalFileRdd";
	}
	
	protected void compute(JavaSparkContext sc) {
		/**
		 * 1、文件名支持通配符
		 */
		JavaRDD<String> lines = sc.textFile(Constants.filename01);
		
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		
		lineLengths.persist(StorageLevel.MEMORY_ONLY());
		
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		
		this.println(String.format("Sum=%d",totalLength));
	}
	
	public static void main(String[] agrs) {
		new ExternalFileRdd().triggerCompute();
	}
}
