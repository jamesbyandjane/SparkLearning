package jamesby.spark.pojo;

public class User implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
	private String user_id;
	private String name;
	private String friends;
	private Integer review_count;
	private String yelping_since;
	private Integer useful;
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFriends() {
		return friends;
	}
	public void setFriends(String friends) {
		this.friends = friends;
	}
	public Integer getReview_count() {
		return review_count;
	}
	public void setReview_count(Integer review_count) {
		this.review_count = review_count;
	}
	public String getYelping_since() {
		return yelping_since;
	}
	public void setYelping_since(String yelping_since) {
		this.yelping_since = yelping_since;
	}
	public Integer getUseful() {
		return useful;
	}
	public void setUseful(Integer useful) {
		this.useful = useful;
	}
	@Override
	public String toString() {
		return this.getName()+","+this.getFriends()+","+this.getUser_id()+","+this.getYelping_since()+","
				+this.getReview_count()+","+this.getUseful();
	}
}
