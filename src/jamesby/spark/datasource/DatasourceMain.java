package jamesby.spark.datasource;

public class DatasourceMain {
	public static void main(String[] args) {
		if ("DatasourceTask".equals(args[0])) {
			new DatasourceTask().launch();
		}
		
		if ("ParquetSourceTask".equals(args[0])) {
			new ParquetSourceTask().launch();
		}
		
		if ("HiveSourceTask".equals(args[0])) {
			new HiveSourceTask().launch();
		}
	}
}
