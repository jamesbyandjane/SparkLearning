package jamesby.spark.sparksql;

public class Average implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private long sum;
	private long count;
	public Average() {
		
	}
	public Average(long sum,long count) {
		this.sum = sum;
		this.count = count;
	}
	public long getSum() {
		return sum;
	}
	public void setSum(long sum) {
		this.sum = sum;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
}
