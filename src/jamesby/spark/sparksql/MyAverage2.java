package jamesby.spark.sparksql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MyAverage2 extends UserDefinedAggregateFunction {
	  private static final long serialVersionUID = 1L;
	  private StructType inputSchema;
	  private StructType bufferSchema;

	  public MyAverage2() {
		  List<StructField> inputFields = new ArrayList<>();
		  inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
		  inputSchema = DataTypes.createStructType(inputFields);

		  List<StructField> bufferFields = new ArrayList<>();
		  bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
		  bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
		  bufferSchema = DataTypes.createStructType(bufferFields);
	  }

	  @Override
	  public StructType inputSchema() {
		  return inputSchema;
	  }

	  @Override
	  public StructType bufferSchema() {
		  return bufferSchema;
	  }

	  @Override
	  public DataType dataType() {
		  return DataTypes.DoubleType;
	  }

	  @Override
	  public boolean deterministic() {
		  return true;
	  }

	  @Override
	  public void initialize(MutableAggregationBuffer buffer) {
		  buffer.update(0, 0L);
		  buffer.update(1, 0L);
	  }

	  @Override
	  public void update(MutableAggregationBuffer buffer, Row input) {
		  if (!input.isNullAt(0)) {
			  long updatedSum = buffer.getLong(0) + input.getLong(0);
			  long updatedCount = buffer.getLong(1) + 1;
			  buffer.update(0, updatedSum);
			  buffer.update(1, updatedCount);
		  }
	  }

	  @Override
	  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		  long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
		  long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
		  buffer1.update(0, mergedSum);
		  buffer1.update(1, mergedCount);
	  }

	  @Override
	  public Double evaluate(Row buffer) {
		  return ((double) buffer.getLong(0)) / buffer.getLong(1);
	  }
}
