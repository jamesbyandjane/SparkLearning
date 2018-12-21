package jamesby.spark.sparksql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkDataSetTask {
	private static Logger logger = Logger.getLogger(SparkDataFrameTask.class);
	public void compute() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			User user = new User();
			user.setUser_id("01");
			user.setName("Íõ±øÑô");
			user.setFriends("");
			user.setReview_count(10);
			user.setUseful(0);
			user.setYelping_since("2018-01-01");
			Encoder<User> userEncoder = Encoders.bean(User.class);
			Dataset<User> javaBeanDS = session.createDataset(
			  Collections.singletonList(user),
			  userEncoder
			);
			javaBeanDS.show();
			
			Encoder<Integer> integerEncoder = Encoders.INT();
			Dataset<Integer> primitiveDS = session.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
			Dataset<Integer> transformedDS = primitiveDS.map(
			    (MapFunction<Integer, Integer>) value -> value + 1,
			    integerEncoder);
			transformedDS.collect();

			Dataset<User> userDS = session.read().json(SparkSqlConstants._FILE_NAME).as(userEncoder);
			userDS.show();			
			
			
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
	public void rddTransToDataFrame() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			JavaRDD<User> userRDD = session.read()
					  .textFile(SparkSqlConstants.filename02)
					  .javaRDD()
					  .map(line -> {
					    String[] parts = line.split(",");
					    User user = new User();
					    user.setUser_id(parts[0]);
					    user.setName(parts[1]);
					    user.setFriends(parts[2]);
					    user.setReview_count(Integer.parseInt(parts[3]));
					    user.setUseful(Integer.parseInt(parts[4]));
					    user.setYelping_since(parts[5]);
					    return user;
					  });
			
			
			Dataset<Row> userDF = session.createDataFrame(userRDD, User.class);
			userDF.createOrReplaceTempView("user");
			
			Dataset<Row> sqlDF = session.sql("SELECT name FROM user WHERE review_count BETWEEN 0 AND 19");

			Encoder<String> stringEncoder = Encoders.STRING();
			
			Dataset<String> teenagerNamesByIndexDF = sqlDF.map(
			    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
			    stringEncoder);
			teenagerNamesByIndexDF.show();
			
			Dataset<String> teenagerNamesByFieldDF = sqlDF.map(
				    (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
				    stringEncoder);
			
			teenagerNamesByFieldDF.show();
				
			
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
	public void testSchema() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			JavaRDD<String> userRDD = session.sparkContext()
			  .textFile(SparkSqlConstants.filename02, 1)
			  .toJavaRDD();
			
			String schemaString = "user_id name friends review_count useful yelping_since";
			List<StructField> fields = new ArrayList<>();
			for (String fieldName : schemaString.split(" ")) {
				if ("review_count".equals(fieldName) || "useful".equals(fieldName)) {
					StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

					fields.add(field);
				}else {
					StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
					fields.add(field);					
				}
			}
			StructType schema = DataTypes.createStructType(fields);			
			
			JavaRDD<Row> rowRDD = userRDD.map((Function<String, Row>) record -> {
			  String[] attrs = record.split(",");
			  return RowFactory.create(attrs[0], attrs[1], attrs[2], attrs[3], attrs[4], attrs[5]);
			});


			Dataset<Row> userDataFrame = session.createDataFrame(rowRDD, schema);

			userDataFrame.createOrReplaceTempView("user");

			Dataset<Row> results = session.sql("SELECT name FROM user");


			Dataset<String> namesDS = results.map(
			    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
			    Encoders.STRING());
			namesDS.show();			
			
			namesDS.count();
			
			namesDS.distinct().show();
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
}
