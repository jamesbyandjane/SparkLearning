package jamesby.spark.rdd.helloword;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectionRdd extends BaseRdd {
	public String getAppName() {
		return "CollectionRdd";
	}
	
	public void compute(JavaSparkContext sc) {
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data,2);
		Integer count = distData.reduce((a, b) -> a + b);
		
		this.println(String.format("Sum=%d",count));
	}
	
	public static void main(String[] args) {
		new CollectionRdd().triggerCompute();
	}
}
