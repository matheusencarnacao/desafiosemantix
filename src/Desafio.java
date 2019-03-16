import java.util.ArrayList;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Desafio {
	
	private static final String LOG_JUL = "access_log_Jul95.txt";
	private static final String LOG_AUG = "access_log_Aug95.txt";
	
	private static Integer numeroHostsUnicos(JavaRDD<String> requests) {
		JavaPairRDD<String, Integer> hosts = requests
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" - - ")[0], 1));
		JavaPairRDD<String, Integer> hostsReduced = hosts.reduceByKey((x,y) -> x + y);
		
		Integer totalHostsUnicos = (int) hostsReduced.filter(x -> x._2 == 1).count();
		
		return totalHostsUnicos;
	}
	
	private static JavaRDD<String> erros404(JavaRDD<String> requests){
		return requests.filter(r -> {
			try {
				String status = r.split("\"(.*?)\" ")[1];
				Integer statusCode = Integer.parseInt(status.split(" ")[0]);
				return statusCode == 404;
			} catch(Exception e) {
				return false;
			}
		});
	}
	
	private static ArrayList<String> urlsTop5Erro404(JavaRDD<String> requests){
		List<Tuple2<String, Integer>> requestsReduced = requests.map(s ->{
			String requestInfo = s.split("\\\"\\w{3,4}\\s")[1];
			return requestInfo.split("\\w{4}\\/\\d.\\d")[0];
		})
				.mapToPair(s-> new Tuple2<String, Integer>(s, 1))
				.reduceByKey((x,y) -> x + y)
				.collect();
		
		requestsReduced.sort((Tuple2<String,Integer> t1, Tuple2<String,Integer> t2) -> Integer.compare(t1._2,t2._2));
		
		List<Tuple2<String, Integer>> urls = requestsReduced.subList(0, 5);
		
		ArrayList<String> urlsTop5 = new ArrayList<>();
		
		urls.forEach(t -> urlsTop5.add(t._1));
		
		return urlsTop5;
	}
	
	public static int totalBytes(JavaRDD<String> requests) {
		int totalBytes = 0;
		for(String s : requests.collect()) {
			try {
				totalBytes += Integer.parseInt(s.split("\\s\\d{1,3}\\s")[1]);	
			} catch (Exception e) {
				System.out.println(s.split("\\s\\d{1,3}\\s")[0]);
			}	
		}
		
		return totalBytes;
	}
	
	public static JavaPairRDD<String, Integer> Erros404PorDia(JavaRDD<String> requests404) { 
		JavaPairRDD<String, Integer> rs = requests404.mapToPair(s -> {
			String requestInfo = s.split("\\[")[1];
			String datetime = requestInfo.split("\\:")[0];
			return new Tuple2<String, Integer>(datetime, 1);
		});
		
		return rs.reduceByKey((x,y) -> x+y);
	}
	
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Desafio Semantix");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		
		JavaRDD<String> requestsJuly = ctx.textFile(LOG_JUL);
		JavaRDD<String> requestsAugust = ctx.textFile(LOG_AUG);
		
		JavaRDD<String> allRequests = requestsJuly.union(requestsAugust);
		System.out.println("Numero total de hosts unicos: " + numeroHostsUnicos(allRequests));
		
		
		JavaRDD<String> requests404 = erros404(allRequests);
		System.out.println("Total de erros 404: " + requests404.count());
		
		System.out.println("Top 5 URLs que causaram erro 404: ");
		urlsTop5Erro404(requests404).forEach(u -> System.out.println(u));
		
		JavaPairRDD<String, Integer> requestsByDate = Erros404PorDia(requests404);
		requestsByDate.foreach(s -> System.out.println(s._1 + " - " + s._2));
		
		System.out.println("Total de Bytes: " + totalBytes(allRequests));
		
	}
}
