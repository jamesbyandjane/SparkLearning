package jamesby.spark.rdd.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastTask implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	public int[] compulate() {
		int[] a = null;
		JavaSparkContext sc = BroadcastTools.getSparkContext();
		try {
			Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
			
			a = broadcastVar.value();
			
		}finally {
			BroadcastTools.close(sc);
		}
		return a;
	}
}
