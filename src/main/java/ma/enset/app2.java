package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class app2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("app2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines=sc.textFile("ventes.txt");
        JavaPairRDD<String,Double> ventesannes=lines.filter(line-> line.trim().split(" ").length>=4)
                .mapToPair(line->{
                   String[] champ=line.trim().split(" ");
                   String date=champ[0];
                   String ville=champ[1];
                   String anne=date.split("-")[0];
                   Double prix=Double.parseDouble(champ[3]);
                   return new Tuple2<>(ville+"-"+anne+"-",prix);
                });
        JavaPairRDD<String,Double> total=ventesannes.reduceByKey(Double::sum);
        total.foreach(data->System.out.println(data._1+" "+data._2));
        sc.close();
    }
}
