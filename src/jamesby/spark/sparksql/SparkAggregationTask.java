package jamesby.spark.sparksql;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import static org.apache.spark.sql.functions.col;

public final class SparkAggregationTask {
	private static Logger logger = Logger.getLogger(SparkDataFrameTask.class);
	public void compute() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
			Dataset<Employee> ds = session.read().json(SparkSqlConstants.filename03).as(employeeEncoder);
			ds.show();

			MyAverage myAverage = new MyAverage();

			TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
			Dataset<Double> result = ds.select(averageSalary);
			
			result.show();	
			
			ds.select(col("name"),col("salary")).show();
			
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
	public void test() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {		
			session.udf().register("myAverage2", new MyAverage2());
	
			Dataset<Row> df = session.read().json(SparkSqlConstants.filename03);
			df.createOrReplaceTempView("employees");
			df.show();
	
			Dataset<Row> result = session.sql("SELECT myAverage2(salary) as average_salary FROM employees");

			result.show();	
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
}
