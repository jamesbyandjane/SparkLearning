package jamesby.spark.rdd.transformation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import scala.Tuple2;

public class TransformationGroupByTask implements java.io.Serializable{

	private static final long serialVersionUID = 1L;
	public List<Tuple2<String,List<String>>> complute() {
		List<Tuple2<String,List<String>>> compulateList = new ArrayList<>();
		JavaSparkContext sc = TransformationTools.getSparkContext();
		try {
			//读取文本文件
			JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
			

			//缓存
			lines.persist(StorageLevel.MEMORY_ONLY());
			
			//取消缓存
			lines.unpersist(); 
			
			//Map变换
			JavaRDD<TransformationUser> maplist = lines.map(x->JSON.parseObject(x, new TypeReference<TransformationUser>() {}));
						
			
			//union，不去重复
			JavaRDD<String> unionlines = lines.union(lines);	
			compulateList.add(new Tuple2<String,List<String>>("union",unionlines.collect()));
			
			
			//intersection,去掉重复
			JavaRDD<TransformationUser> intersectionList = maplist.intersection(maplist);
			List<TransformationUser> intersectionListCollect = intersectionList.collect();
			List<String> intersectionListCollectStr = new ArrayList<>();
			intersectionListCollect.forEach(x->intersectionListCollectStr.add(x.toString()));
			compulateList.add(new Tuple2<String,List<String>>("intersection",intersectionListCollectStr));
			
			//distinct
			JavaRDD<String> distinctList = unionlines.distinct();
			compulateList.add(new Tuple2<String,List<String>>("distinct",distinctList.collect()));
			
			JavaPairRDD<String, Integer> mapToPairList = maplist.mapToPair(x->{
				return new Tuple2<String,Integer>(x.getYelping_since(),x.getReview_count());
			});
			
			JavaPairRDD<String, String> mapToPairList1 = maplist.mapToPair(x->{
				return new Tuple2<String,String>(x.getUser_id(),x.getFriends());
			});		
			
			JavaPairRDD<String, String> mapToPairList2 = maplist.mapToPair(x->{
				return new Tuple2<String,String>(x.getUser_id(),x.getName());
			});				
			
			JavaPairRDD<String,Integer> aggregateByKeyResult = mapToPairList.aggregateByKey(0, 
				new Function2<Integer,Integer,Integer>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						if (v1>v2)
							return v1;
						return v2;
					}
				}, 
				new Function2<Integer,Integer,Integer>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1+v2;
					}
				});
			List<Tuple2<String,Integer>> aggregateByKeyCollect = aggregateByKeyResult.collect();
			List<String> aggregateByKeyCollectStr = new ArrayList<>();
			aggregateByKeyCollect.forEach(x->aggregateByKeyCollectStr.add(x._1+":"+x._2()));
			
			compulateList.add(new Tuple2<String,List<String>>("aggregateByKey",aggregateByKeyCollectStr));
						
			
			JavaPairRDD<String,Integer> sortByKeyList = mapToPairList.sortByKey();
			List<Tuple2<String,Integer>> sortByKeyListCollect = sortByKeyList.collect();
			List<String> sortByKeyListStr = new ArrayList<>();
			sortByKeyListCollect.forEach(x->sortByKeyListStr.add(x._1+":"+x._2()));			
			
			compulateList.add(new Tuple2<String,List<String>>("sortByKey",sortByKeyListStr));
			
			
			JavaPairRDD<String, Tuple2<String, String>> joinList = mapToPairList1.join(mapToPairList2);
			List<Tuple2<String,Tuple2<String,String>>> joinListCollect = joinList.collect();
			List<String> joinListCollectStr = new ArrayList<>();
			joinListCollect.forEach(x->joinListCollectStr.add(x._1+":"+x._2()._1+","+x._2()._2));	
			compulateList.add(new Tuple2<String,List<String>>("join",joinListCollectStr));
			
			JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> coGroupList = mapToPairList1.cogroup(mapToPairList2);
			List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> coGroupListCollect = coGroupList.collect();
			List<String> coGroupListCollectStr = new ArrayList<>();
			coGroupListCollect.forEach(x->{
				StringBuffer buf = new StringBuffer(x._1);
				buf.append("#");
				
				Iterator<String> listA = x._2._1.iterator();
				while(listA.hasNext()) {
					buf.append(","+listA.next());
				}
				buf.append("#");
				Iterator<String> listB = x._2._2.iterator();
				while(listB.hasNext()) {
					buf.append(","+listB.next());
				}		
				coGroupListCollectStr.add(buf.toString());
			});
			compulateList.add(new Tuple2<String,List<String>>("cogroup",coGroupListCollectStr));
			
			
			JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> cartesianList = mapToPairList1.cartesian(mapToPairList2);
			List<String> cartesianListStr = new ArrayList<>();
			List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>  cartesianListCollect = cartesianList.collect();
			cartesianListCollect.forEach(x->{
				cartesianListStr.add(x._1._1+","+x._1._2+","+x._2._1+","+x._2._2);
			});
			compulateList.add(new Tuple2<String,List<String>>("cartesian",cartesianListStr));
			
			JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> coalesceList = cartesianList.coalesce(3);
			List<String> coalesceListArray = new ArrayList<>();
			List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> list = coalesceList.collect();
			list.forEach(x->{
				coalesceListArray.add(x._1._1+","+x._1._2+","+x._2._1+","+x._2._2);
			});
			compulateList.add(new Tuple2<String,List<String>>("coalesce",coalesceListArray));
			
			
			
			JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> repartitionList = cartesianList.repartition(3);
			List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> list2 = repartitionList.collect();
			List<String> list2Array = new ArrayList<>();
			list2.forEach(x->{
				list2Array.add(x._1._1+","+x._1._2+","+x._2._1+","+x._2._2);
			});
			compulateList.add(new Tuple2<String,List<String>>("repartition",list2Array));
						
		}finally {
			TransformationTools.close(sc);
		}
		return compulateList;
	}
}
