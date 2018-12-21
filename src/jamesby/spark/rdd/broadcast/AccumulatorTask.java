package jamesby.spark.rdd.broadcast;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class AccumulatorTask implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	public long compulate() {
		long a = 0;
		JavaSparkContext sc = BroadcastTools.getSparkContext();
		try {
			LongAccumulator accum = sc.sc().longAccumulator();

			sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));

			a = accum.value();
		}finally {
			BroadcastTools.close(sc);
		}
		return a;
	}

}
