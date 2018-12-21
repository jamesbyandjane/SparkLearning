package jamesby.spark.rdd.helloword;

import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;

public class InlineFunctionRddMain {
	private static Logger logger = Logger.getLogger(InlineFunctionRddMain.class);
	public static void main(String[] args) {
		int ret = new InlineFunctionRdd().triggerCompute();
		Log.info("--------因为用了内联，所以必须所有外部类的属性都是序列化的，结果为："+ret+"-------------------------");
	}
}
