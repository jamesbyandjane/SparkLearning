package jamesby.spark.rdd.helloword;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class CustomFunctionRdd extends BaseRdd{
	public String getAppName() {
		return "CustomFunctionRdd";
	}
	
	public void compute(JavaSparkContext sc) {

		JavaRDD<String> lines = sc.textFile(Constants.filename01);
		
		JavaRDD<Integer> lineLengths = lines.map(new CustomGetLengthFunction());
		
		lineLengths.persist(StorageLevel.MEMORY_ONLY());
		
		int totalLength = lineLengths.reduce(new CustomSumFunction());
		println(String.format("Sum=%d", totalLength));
	}
	
	public static void main(String[] agrs) {
		new CustomFunctionRdd().triggerCompute();
	}
}
