package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class app1 {
 public static void main(String[] args) {
     SparkConf conf=new SparkConf().setAppName("app1").setMaster("spark://spark-master:7077");
     JavaSparkContext sc=new JavaSparkContext(conf);

     JavaRDD<String> lines= sc.textFile("/opt/bitnami/spark/data/ventes.txt");

     JavaPairRDD<String,Double> villes= lines.filter(line -> line.trim().split(" ").length >= 4)
             .mapToPair(line-> {
         String[] champ=line.trim().split(" ");
         String ville=champ[1];
         Double prix=Double.parseDouble(champ[3]);
         return new Tuple2<>(ville,prix);
     });

     JavaPairRDD<String,Double> totalvilles=villes.reduceByKey(Double::sum);

     totalvilles.foreach(data-> System.out.println(data._1()+":"+data._2()));
     sc.close();
 }
}
