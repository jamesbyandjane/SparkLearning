package jamesby.spark.rdd.helloword;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 1¡¢JavaSparkContext.wholeTextFiles lets you read a directory containing multiple small text files, 
 * and returns each of them as (filename, content) pairs. This is in contrast with textFile, 
 * which would return one record per line in each file.
 * 
 * 2¡¢For SequenceFiles, use SparkContext¡¯s sequenceFile[K, V] method where K and V are the types of 
 * key and values in the file. These should be subclasses of Hadoop¡¯s Writable interface, 
 * like IntWritable and Text.
 * 
 * 3¡¢For other Hadoop InputFormats, you can use the JavaSparkContext.hadoopRDD method, 
 * which takes an arbitrary JobConf and input format class, key class and value class.
 * Set these the same way you would for a Hadoop job with your input source. 
 * You can also use JavaSparkContext.newAPIHadoopRDD for InputFormats based on 
 * the ¡°new¡± MapReduce API (org.apache.hadoop.mapreduce).
 * 
 * 4¡¢JavaRDD.saveAsObjectFile and JavaSparkContext.objectFile support saving an RDD in a simple format 
 * consisting of serialized Java objects. While this is not as efficient as specialized formats 
 * like Avro, it offers an easy way to save any RDD.
 * 
 * 5¡¢To print all elements on the driver, one can use the collect() method to first bring the RDD 
 * to the driver node thus: rdd.collect().foreach(println). This can cause the driver to 
 * run out of memory, though, because collect() fetches the entire RDD to a single machine; 
 * if you only need to print a few elements of the RDD, a safer approach is to use the take(): 
 * rdd.take(100).foreach(println).
 *
 */

public abstract class BaseRdd implements java.io.Serializable{
	private static Logger logger = Logger.getLogger(InlineFunctionRddMain.class);
	
	abstract String getAppName();
	
	protected void triggerCompute() {
		JavaSparkContext sc = getJavaSparkContext();
		compute(sc);
		sc.close();
	}
	
	abstract void compute(JavaSparkContext sc);
	
	protected void println(String str) {
		logger.info(String.format("---------------------%s:%s--------------------", getAppName(),str));
	}
	
	private JavaSparkContext getJavaSparkContext(){
		SparkConf conf = new SparkConf().setAppName(getAppName()).setMaster(Constants._MASTER);
		JavaSparkContext sc = new JavaSparkContext(conf);	
		return sc;
	}
}
