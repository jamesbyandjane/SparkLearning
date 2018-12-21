package jamesby.spark.rdd.transformation;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import scala.Tuple2;

public class TransformationActionTask implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	public void test() {
		JavaSparkContext sc = TransformationTools.getSparkContext();
		try
		{
			reduce(sc);
			count(sc);
			first(sc);
			take(sc);
			takeSample(sc);
			takeOrdered(sc);
			saveAsTextFile(sc);
			saveAsSequenceFile(sc);
			saveAsObjectFile(sc);
			countByKey(sc);
		}finally {
			TransformationTools.close(sc);
		}
	}
	
	public TransformationUser reduce(JavaSparkContext sc) {
		//读取文本文件
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		
		//Map变换
		JavaRDD<TransformationUser> maplist = lines.map(x->JSON.parseObject(x, new TypeReference<TransformationUser>() {}));
		return maplist.reduce((p1,p2)->{
			p1.setReview_count(p1.getReview_count()+p2.getReview_count());
			return p1;
		});
	}
	
	public long count(JavaSparkContext sc) {
		//读取文本文件
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		return lines.count();
	}
	
	public String first(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		return lines.first();
	}
	
	public List<String> take(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		return lines.take(20);
	}
	
	public List<String> takeSample(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		return lines.takeSample(false,10);
	}
	
	public List<String> takeOrdered(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		return lines.takeOrdered(10);
	}
	
	public void saveAsTextFile(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		lines.saveAsTextFile(TransformationConstants._FILE_PATH+"/002/");
	}
	
	public void saveAsSequenceFile(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		//Map变换
		JavaRDD<TransformationUser> maplist = lines.map(x->JSON.parseObject(x, new TypeReference<TransformationUser>() {}));
		JavaPairRDD<String, Integer> mapToPairList = maplist.mapToPair(x->{
			return new Tuple2<String,Integer>(x.getYelping_since(),x.getReview_count());
		});

		
	}
	
	public void saveAsObjectFile(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		lines.saveAsObjectFile(TransformationConstants._FILE_PATH+"/001/");
	}
	
	public Map<String,Long> countByKey(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
		//Map变换
		JavaRDD<TransformationUser> maplist = lines.map(x->JSON.parseObject(x, new TypeReference<TransformationUser>() {}));
		JavaPairRDD<String, Integer> mapToPairList = maplist.mapToPair(x->{
			return new Tuple2<String,Integer>(x.getYelping_since(),x.getReview_count());
		});
		return mapToPairList.countByKey();
	}
}
