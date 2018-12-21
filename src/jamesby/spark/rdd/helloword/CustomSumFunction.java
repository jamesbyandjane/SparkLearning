package jamesby.spark.rdd.helloword;

import org.apache.spark.api.java.function.Function2;

public class CustomSumFunction implements Function2<Integer, Integer, Integer> {
	private static final long serialVersionUID = 1L;

	public Integer call(Integer a, Integer b) { 
		return a + b; 
	}
}
