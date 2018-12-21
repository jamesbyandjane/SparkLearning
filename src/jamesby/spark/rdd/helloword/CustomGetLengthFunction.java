package jamesby.spark.rdd.helloword;

import org.apache.spark.api.java.function.Function;

public class CustomGetLengthFunction implements Function<String, Integer> {
	private static final long serialVersionUID = 1L;

	public Integer call(String s) { 
		return s.length(); 
	}
}
