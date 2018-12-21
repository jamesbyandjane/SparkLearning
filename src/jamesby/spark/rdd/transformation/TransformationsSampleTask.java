package jamesby.spark.rdd.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TransformationsSampleTask implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	public List<Tuple2<String,List<String>>> complute() {
		List<Tuple2<String,List<String>>> compulateList = new ArrayList<>();
		JavaSparkContext sc = TransformationTools.getSparkContext();
		try {
			//读取文本文件
			JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
			
			/**
			 * 抽样之后被放回，可以多次抽样。
			 */
			JavaRDD<String> trueSampleLines = lines.sample(true,0.1);
			
			/**
			 * 抽样之后不放回。
			 */
			JavaRDD<String> falseSampleLines = lines.sample(false,0.1);
			
			compulateList.add(new Tuple2<String,List<String>>("sample[true]",trueSampleLines.collect()));
			compulateList.add(new Tuple2<String,List<String>>("sample[false]",falseSampleLines.collect()));
			
		}finally {
			TransformationTools.close(sc);
		}
		return compulateList;
	}
}
