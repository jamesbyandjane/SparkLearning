package jamesby.spark.sparksql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

public class MyAverage extends Aggregator<Employee, Average, Double> {
	  private static final long serialVersionUID = 1L;

	  @Override  
	  public Average zero() {
	    return new Average(0L, 0L);
	  }

	  @Override
	  public Average reduce(Average buffer, Employee employee) {
	    long newSum = buffer.getSum() + employee.getSalary();
	    long newCount = buffer.getCount() + 1;
	    buffer.setSum(newSum);
	    buffer.setCount(newCount);
	    return buffer;
	  }

	  @Override
	  public Average merge(Average b1, Average b2) {
	    long mergedSum = b1.getSum() + b2.getSum();
	    long mergedCount = b1.getCount() + b2.getCount();
	    b1.setSum(mergedSum);
	    b1.setCount(mergedCount);
	    return b1;
	  }

	  @Override
	  public Double finish(Average reduction) {
	    return ((double) reduction.getSum()) / reduction.getCount();
	  }

	  @Override
	  public Encoder<Average> bufferEncoder() {
	    return Encoders.bean(Average.class);
	  }

	  @Override
	  public Encoder<Double> outputEncoder() {
	    return Encoders.DOUBLE();
	  }
}