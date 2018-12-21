package jamesby.spark.rdd.helloword;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class KeyValuePairRdd extends BaseRdd{
	public String getAppName() {
		return this.getClass().getName();
	}
	
	protected void compute(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(Constants.filename01);
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		List<Tuple2<String,Integer>> collectList =  counts.sortByKey().collect();
		collectList.forEach(x->{
			println(String.format("Key=%s,Value=%d", x._1,x._2));
		});
	}
	public static void main(String[] args) {
		new KeyValuePairRdd().triggerCompute();
	}
}
