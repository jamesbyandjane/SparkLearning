package jamesby.spark.sparksql;

public class Employee implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private String name;
	private long salary;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getSalary() {
		return salary;
	}
	public void setSalary(long salary) {
		this.salary = salary;
	}
}
