package jamesby.spark.rdd.helloword;

import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;

public class InlineFunctionRddMain {
	private static Logger logger = Logger.getLogger(InlineFunctionRddMain.class);
	public static void main(String[] args) {
		int ret = new InlineFunctionRdd().triggerCompute();
		Log.info("--------��Ϊ�������������Ա��������ⲿ������Զ������л��ģ����Ϊ��"+ret+"-------------------------");
	}
}
