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
			//��ȡ�ı��ļ�
			JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
			
			/**
			 * ����֮�󱻷Żأ����Զ�γ�����
			 */
			JavaRDD<String> trueSampleLines = lines.sample(true,0.1);
			
			/**
			 * ����֮�󲻷Żء�
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
