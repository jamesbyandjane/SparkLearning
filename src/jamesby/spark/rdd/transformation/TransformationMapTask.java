package jamesby.spark.rdd.transformation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import scala.Tuple2;

public class TransformationMapTask implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	public List<Tuple2<String,List<String>>> compulate() {
		
		List<Tuple2<String,List<String>>> compulateList = new ArrayList<>();
		
		JavaSparkContext sc = TransformationTools.getSparkContext();
		try {
			//读取文本文件
			JavaRDD<String> lines = sc.textFile(TransformationConstants._FILE_NAME);
			
			//Map变换
			JavaRDD<TransformationUser> maplist = lines.map(x->JSON.parseObject(x, new TypeReference<TransformationUser>() {}));
			
			//Filter变换
			JavaRDD<TransformationUser> filterlist = maplist.filter(x->!"None".equals(x.getFriends()));
			
			//FlatMap变换
			JavaRDD<String> flatMapList = maplist.flatMap(x->{
				List<String> list = new ArrayList<>();
				if (!"None".equals(x.getFriends())) {
					list.add(x.getUser_id()+","+x.getFriends());
				}
				return list.iterator();
			});
			
			List<String> flatMapListResult = flatMapList.collect();
			
			compulateList.add(new Tuple2<String,List<String>>("flatMap",flatMapListResult));
			
			List<TransformationUser> filterResult = filterlist.collect();
			List<String> filterResultList = new ArrayList<>();
			filterResult.forEach(x->filterResultList.add(x.getName()));
			
			compulateList.add(new Tuple2<String,List<String>>("filter",filterResultList));
			
			//mapPartitions变化
			JavaRDD<String> mapPartitionsList = maplist.mapPartitions(new FlatMapFunction<Iterator<TransformationUser>,String>(){

				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<String> call(Iterator<TransformationUser> t) throws Exception {
					List<String> list = new ArrayList<>();
					while(t.hasNext()) {
						TransformationUser user = t.next();
						list.add(user.getName()+","+user.getFriends());
					}
					return list.iterator();
				}
			});
			
			List<String> mapPartitionsListResult = mapPartitionsList.collect();
			
			compulateList.add(new Tuple2<String,List<String>>("mapPartitions",mapPartitionsListResult));
			
			//mapPartitionsWithIndex变换
			JavaRDD<String> mapPartitionsWithIndexList = maplist.mapPartitionsWithIndex(new Function2<Integer,Iterator<TransformationUser>,Iterator<String>>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Iterator<String> call(Integer v1, Iterator<TransformationUser> t) throws Exception {
					List<String> list = new ArrayList<>();
					while(t.hasNext()) {
						TransformationUser user = t.next();
						list.add(user.getName()+","+user.getFriends());
					}
					return list.iterator();						
				}
				
			},false);
			
			List<String> mapPartitionsWithIndexListResult = mapPartitionsWithIndexList.collect();
			
			compulateList.add(new Tuple2<String,List<String>>("mapPartitionsWithIndex",mapPartitionsWithIndexListResult));
		}finally {
			TransformationTools.close(sc);
		}
		return compulateList;
		
	}
}
