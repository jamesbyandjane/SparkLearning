package jamesby.spark.rdd.transformation;

import java.util.List;

import org.apache.log4j.Logger;

import scala.Tuple2;

public class TransformationMain {
	private static Logger logger = Logger.getLogger(TransformationTools.class);
	public static void main(String[] args) {
		if (null==args||args.length!=1)
			throw new RuntimeException("参数未传入");
		
		String arg0 = args[0];
		if ("TransformationMapTask".equals(arg0)) {
			List<Tuple2<String,List<String>>> computeList = new TransformationMapTask().compulate();
			computeList.forEach(x->{
				logger.info("*******************BEGIN:"+x._1+"*******************");
				x._2.forEach(y->{
					
					logger.info(y);
					
				});
				logger.info("*******************END:"+x._1+"*******************");
			});
		}
		
		if ("TransformationsSampleTask".equals(arg0)) {
			List<Tuple2<String,List<String>>> computeList = new TransformationsSampleTask().complute();
			computeList.forEach(x->{
				logger.info("*******************BEGIN:"+x._1+"*******************");
				x._2.forEach(y->{
					
					logger.info(y);
					
				});
				logger.info("*******************END:"+x._1+"*******************");
			});
		}
		
		if ("TransformationGroupByTask".equals(arg0)) {
			List<Tuple2<String,List<String>>> computeList = new TransformationGroupByTask().complute();
			computeList.forEach(x->{
				logger.info("*******************BEGIN:"+x._1+"*******************");
				x._2.forEach(y->{
					
					logger.info(y);
					
				});
				logger.info("*******************END:"+x._1+"*******************");
			});			
		}
		
		if ("TransformationActionTask".equals(arg0)) {
			new TransformationActionTask().test();
			logger.info("*******************TransformationActionTask*******************");		
		}		
	}
}
