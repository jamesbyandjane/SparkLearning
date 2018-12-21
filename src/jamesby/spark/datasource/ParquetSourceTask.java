package jamesby.spark.datasource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ParquetSourceTask {
	public void launch() {
		SparkSession session = SparkSessionUtils.getSparkSession();
		try {
			loadInfoFromParquet(session);
			parquetPartiton(session);
		}finally {
			SparkSessionUtils.closeSparkSession(session);
		}
	}
	
	public void loadInfoFromParquet(SparkSession session) {
		Dataset<Row> peopleDF = session.read().json(Constants.filename03);

		peopleDF.write().mode(SaveMode.Overwrite).parquet(Constants._FILE_PATH+ "/parquet_employee.parquet");

		Dataset<Row> parquetFileDF = session.read().parquet(Constants._FILE_PATH+ "/parquet_employee.parquet");

		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = session.sql("SELECT name FROM parquetFile WHERE salary BETWEEN 1 AND 190000");
		Dataset<String> namesDS = namesDF.map(
		    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
		    Encoders.STRING());
		namesDS.show();		
	}
	
	public void parquetPartiton(SparkSession session) {
		List<Square> squares = new ArrayList<>();
		for (int value = 1; value <= 5; value++) {
		  Square square = new Square();
		  square.setValue(value);
		  square.setSquare(value * value);
		  squares.add(square);
		}

		Dataset<Row> squaresDF = session.createDataFrame(squares, Square.class);
		squaresDF.write().parquet(Constants._FILE_PATH+"/data/test_table/key=1");

		List<Cube> cubes = new ArrayList<>();
		for (int value = 6; value <= 10; value++) {
		  Cube cube = new Cube();
		  cube.setValue(value);
		  cube.setCube(value * value * value);
		  cubes.add(cube);
		}

		Dataset<Row> cubesDF = session.createDataFrame(cubes, Cube.class);
		cubesDF.write().parquet(Constants._FILE_PATH+"/data/test_table/key=2");

		Dataset<Row> mergedDF = session.read().option("mergeSchema", true).parquet(Constants._FILE_PATH+"/data/test_table");
		mergedDF.printSchema();

	}
	
	public static class Square implements Serializable {

		private static final long serialVersionUID = 1L;
		private int value;
	    private int square;
		
	    public int getValue() {
			return value;
		}
		public void setValue(int value) {
			this.value = value;
		}
		public int getSquare() {
			return square;
		}
		public void setSquare(int square) {
			this.square = square;
		}
	}	
	
	public static class Cube implements Serializable {
		private static final long serialVersionUID = 1L;
		private int value;
		private int cube;
		public int getValue() {
			return value;
		}
		public void setValue(int value) {
			this.value = value;
		}
		public int getCube() {
			return cube;
		}
		public void setCube(int cube) {
			this.cube = cube;
		}
	}	
}
