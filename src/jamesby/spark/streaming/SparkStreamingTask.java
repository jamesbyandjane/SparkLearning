package jamesby.spark.streaming;

import java.util.Arrays;
import java.util.UUID;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import jamesby.spark.utils.SparkSessionUtils;

public class SparkStreamingTask implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	public void compute() throws StreamingQueryException {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {
			session.sparkContext().setLogLevel("WARN");
			Dataset<Row> lines = session
					 .readStream()
					 .format("socket")
					 .option("host", "localhost")
					 .option("port", 9999)
					 .load();

			lines.isStreaming();

			lines.printSchema();			
			
			Dataset<String> words = lines
					.as(Encoders.STRING())
					.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

			Dataset<Row> wordCounts = words.groupBy("value").count();
			
			StreamingQuery query = wordCounts.writeStream()
			  .outputMode("complete")
			  .format("console")
			  .start();

			query.awaitTermination();			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
	public void readCsvFile() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			
			StructType userSchema = new StructType().add("name", "string").add("age", "integer");
			Dataset<Row> csvDF = session
			  .readStream()
			  .option("sep", ";")
			  .schema(userSchema)      
			  .csv("/path/to/directory");			
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}		
		
	}

	public void deviceDataTest() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			
			Dataset<Row> df = null;    
			Dataset<DeviceData> ds = df.as(ExpressionEncoder.javaBean(DeviceData.class));

			df.select("device").where("signal > 10"); 
			ds.filter((FilterFunction<DeviceData>) value -> value.getSignal() > 10)
			  .map((MapFunction<DeviceData, String>) value -> value.getDevice(), Encoders.STRING());

			df.groupBy("deviceType").count();

			ds.groupByKey((MapFunction<DeviceData, String>) value -> value.getDeviceType(), Encoders.STRING())
			  .agg(typed.avg((MapFunction<DeviceData, Double>) value -> value.getSignal()));		

			df.createOrReplaceTempView("updates");
			session.sql("select count(*) from updates"); 
			df.isStreaming();
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}			
	}
	
	public void testWindow() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			Dataset<Row> words = null;

			Dataset<Row> windowedCounts = words.groupBy(
					functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
					  words.col("word")).count();
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
	public void testWindowWithWaterMarker() {
		
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			Dataset<Row> words = null;

			Dataset<Row> windowedCounts = words
			    .withWatermark("timestamp", "10 minutes")
			    .groupBy(
			        functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
			        words.col("word"))
			    .count();
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}		
	}
	
	public void testStreamJoinDf() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			Dataset<Row> staticDf = null;//session.read(). ...;
			Dataset<Row> streamingDf = null;//session.readStream(). ...;
			streamingDf.join(staticDf, "type");         // inner equi-join with a static DF
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}			
	}
	
	public void testStreamJoinStream() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			Dataset<Row> impressions = null;// spark.readStream(). ...
			Dataset<Row> clicks = null;//spark.readStream(). ...

			Dataset<Row> impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours");
			Dataset<Row> clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours");

			// Join with event-time constraints
			impressionsWithWatermark.join(
			  clicksWithWatermark,
			  expr(
			    "clickAdId = impressionAdId AND " +
			    "clickTime >= impressionTime AND " +
			    "clickTime <= impressionTime + interval 1 hour ")
			);
			
			impressionsWithWatermark.join(
					  clicksWithWatermark,
					  expr(
					    "clickAdId = impressionAdId AND " +
					    "clickTime >= impressionTime AND " +
					    "clickTime <= impressionTime + interval 1 hour "),
					  "leftOuter"                 // can be "inner", "leftOuter", "rightOuter"
					);
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}			
	}	
	
	public void testDeduplication() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			
			Dataset<Row> streamingDf = null;//spark.readStream(). ...;  // columns: guid, eventTime, ...

			// Without watermark using guid column
			streamingDf.dropDuplicates("guid");

			// With watermark using guid and eventTime columns
			streamingDf
			  .withWatermark("eventTime", "10 seconds")
			  .dropDuplicates("guid", "eventTime");
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}		
	}
	
	public void testSinkToParquet() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			Dataset<Row> writeStream = null;
			writeStream.writeStream()
		    .format("parquet")        // can be "orc", "json", "csv", etc.
		    .option("path", "path/to/destination/dir")
		    .start();
			
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}			
	}
	
	public void testSinkToKafka() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {		
			Dataset<Row> writeStream = null;
			writeStream.writeStream()
		    .format("kafka")
		    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
		    .option("topic", "updates")
		    .start();
			
//			writeStream.writeStream()
//		    .foreach()
//		    .start();
			
//			writeStream
//		    .format("console")
//		    .start()			
			
//			writeStream
//		    .format("memory")
//		    .queryName("tableName")
//		    .start()		
			
//			streamingDatasetOfString.writeStream().foreachBatch(
//					  new VoidFunction2<Dataset<String>, Long> {
//					    public void call(Dataset<String> dataset, Long batchId) {
//					      // Transform and write batchDF
//					    }    
//					  }
//					).start();			
			
//			streamingDatasetOfString.writeStream().foreach(
//					  new ForeachWriter[String] {
//					    @Override public boolean open(long partitionId, long version) {
//					      // Open connection
//					    }
//					    @Override public void process(String record) {
//					      // Write string to connection
//					    }
//					    @Override public void close(Throwable errorOrNull) {
//					      // Close the connection
//					    }
//					  }
//					).start();			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}			
	}	
	
	public void testSinkExample() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
		try {			
			Dataset<Row> deviceDataDf = null;
			Dataset<Row> noAggDF = deviceDataDf.select("device").where("signal > 10");
	
			// Print new data to console
			noAggDF
			  .writeStream()
			  .format("console")
			  .start();
	
			// Write new data to Parquet files
			noAggDF
			  .writeStream()
			  .format("parquet")
			  .option("checkpointLocation", "path/to/checkpoint/dir")
			  .option("path", "path/to/destination/dir")
			  .start();
	
			// ========== DF with aggregation ==========
			Dataset<Row> df = null;
			Dataset<Row> aggDF = df.groupBy("device").count();
	
			// Print updated aggregations to console
			aggDF
			  .writeStream()
			  .outputMode("complete")
			  .format("console")
			  .start();
	
			// Have all the aggregates in an in-memory table
			aggDF
			  .writeStream()
			  .queryName("aggregates")    // this query name will be the table name
			  .outputMode("complete")
			  .format("memory")
			  .start();
	
			session.sql("select * from aggregates").show();   // interactively query in-memory table
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}						
	}
	
	public void testTrigger() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		
			try {
				Dataset<Row> df = null;
				
				// Default trigger (runs micro-batch as soon as it can)
				df.writeStream()
				  .format("console")
				  .start();

				// ProcessingTime trigger with two-seconds micro-batch interval
				df.writeStream()
				  .format("console")
				  .trigger(Trigger.ProcessingTime("2 seconds"))
				  .start();

				// One-time trigger
				df.writeStream()
				  .format("console")
				  .trigger(Trigger.Once())
				  .start();

				// Continuous trigger with one-second checkpointing interval
				df.writeStream()
				  .format("console")
				  .trigger(Trigger.Continuous("1 second"))
				  .start();				
				
		}finally {
				SparkSessionUtils.closeSparkSession(session);
		}						
	}
	
	public void testStreamingQuery() throws StreamingQueryException{
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			Dataset<Row> df = null;
			StreamingQuery query = df.writeStream().format("console").start();   // get the query object

			query.id();          // get the unique identifier of the running query that persists across restarts from checkpoint data

			query.runId();       // get the unique id of this run of the query, which will be generated at every start/restart

			query.name();        // get the name of the auto-generated or user-specified name

			query.explain();   // print detailed explanations of the query

			query.stop();      // stop the query

			query.awaitTermination();   // block until query is terminated, with stop() or with error

			query.exception();       // the exception if the query has been terminated with error

			query.recentProgress();  // an array of the most recent progress updates for this query

			query.lastProgress();    // the most recent progress update of this streaming query
			
			session.streams().active();    // get the list of currently active streaming queries

			session.streams().get(UUID.randomUUID());   // get a query object by its unique id

			session.streams().awaitAnyTermination();   // block until any one of them terminates
			
			System.out.println(query.lastProgress());
			
			System.out.println(query.status());
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}		
	}
	
	public void testMetrics() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			session.streams().addListener(new StreamingQueryListener() {
			    @Override
			    public void onQueryStarted(QueryStartedEvent queryStarted) {
			        System.out.println("Query started: " + queryStarted.id());
			    }
			    @Override
			    public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
			        System.out.println("Query terminated: " + queryTerminated.id());
			    }
			    @Override
			    public void onQueryProgress(QueryProgressEvent queryProgress) {
			        System.out.println("Query made progress: " + queryProgress.progress());
			    }
			});
			
			session.conf().set("spark.sql.streaming.metricsEnabled", "true");
			// or
			session.sql("SET spark.sql.streaming.metricsEnabled=true");			
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}				
	}
	
	public void testContinuousStreaming() throws StreamingQueryException{
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			session
			  .readStream()
			  .format("kafka")
			  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
			  .option("subscribe", "topic1")
			  .load()
			  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
			  .writeStream()
			  .format("kafka")
			  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
			  .option("topic", "topic1")
			  .trigger(Trigger.Continuous("1 second"))  // only change in query
			  .start();			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}		
	}	
	
	public void testCheckPoint() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			Dataset<Row> aggDF = null;
			aggDF
			  .writeStream()
			  .outputMode("complete")
			  .option("checkpointLocation", "path/to/HDFS/dir")
			  .format("memory")
			  .start();
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}			
	}
	
	public class DeviceData implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		private String device;
	    private String deviceType;
	    private Double signal;
	    private java.sql.Date time;
		public String getDevice() {
			return device;
		}
		public void setDevice(String device) {
			this.device = device;
		}
		public String getDeviceType() {
			return deviceType;
		}
		public void setDeviceType(String deviceType) {
			this.deviceType = deviceType;
		}
		public Double getSignal() {
			return signal;
		}
		public void setSignal(Double signal) {
			this.signal = signal;
		}
		public java.sql.Date getTime() {
			return time;
		}
		public void setTime(java.sql.Date time) {
			this.time = time;
		}
		  
	}	
}