package jamesby.spark.rdd.broadcast;

import org.apache.log4j.Logger;

public class BroadcastMain {
	private static Logger logger = Logger.getLogger(BroadcastMain.class);
	public static void main(String[] args) {
		if (null==args||args.length!=1)
			throw new RuntimeException("参数未传入");
		
		String arg0 = args[0];
		if ("BroadcastTask".equals(arg0)) {
			int[] array = new BroadcastTask().compulate();
			logger.info("*******************BEGIN:BroadcastMain*****************");
			for(int item:array) {
				logger.info(item);
			}
			logger.info("*******************END:BroadcastMain*****************");
		}
		
		if ("AccumulatorTask".equals(arg0)) {
			long ret = new AccumulatorTask().compulate();
			logger.info("*******************BEGIN:BroadcastMain*****************");
			logger.info(ret);
			logger.info("*******************END:BroadcastMain*****************");
		}		
	}
}
