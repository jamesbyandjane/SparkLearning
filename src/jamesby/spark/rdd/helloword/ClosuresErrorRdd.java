package jamesby.spark.rdd.helloword;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ClosuresErrorRdd extends BaseRdd{
	
	public String getAppName() {
		return "ClosuresErrorRdd";
	}
	
	protected void compute(JavaSparkContext sc) {
		List<Integer> data = Arrays.asList(1,2,3,4,5,6);
		JavaRDD<Integer> rdd = sc.parallelize(data);

		int counter = 0;
		
		/**
		 * 如下代码为错误的计算方式
		 */
		//rdd.foreach(x -> counter += x);

		println("Counter value: " + counter);
	}
	
	public static void main(String[] args) {
		new ClosuresErrorRdd().triggerCompute();
	}
}
