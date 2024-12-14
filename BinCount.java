
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.w3c.dom.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.naming.Context;

import java.util.Iterator;

public final class BinCount {
  
  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: BinCount <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("BinCount")
      .getOrCreate();
   
    int[] chromosomes = new int[]{0, 248956422, 242193529, 198295559, 190214555, 181538259, 170805979, 159345973, 145138636, 138394717, 133797422, 135086622, 133275309, 114364328, 107043718, 101991189, 90338345, 83257441, 80373285, 58617616, 64444167, 46709983, 50818468, 156040895, 57227415}; // index chromosome 1 for index 1, chromosome 2 for index 2, X = 23, Y = 24
   
    int[] bins = new int[]{0, 0, 2490, 4912, 6895, 8798, 10614, 12323, 13917, 15369, 16753, 18091, 19442, 20775, 21919, 22990, 24010, 24914, 25747, 26551, 27138, 27783, 28251, 28760, 30321, 30894}; // index chromosome 1 for index 1, chromosome 2 for index 2, X = 23, Y = 24    
 
    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD(); // each line of input
    
    JavaRDD<String> valid = lines.filter(line -> {
	String[] l = line.split("[ \t]+"); // entries separated by tabs & spaces
	if (l.length != 4) return false; // valid lines must have c1 p1 c2 p2 (4 values)
	return Integer.parseInt(l[1]) <= chromosomes[Integer.parseInt(l[0])] && Integer.parseInt(l[3]) <= chromosomes[Integer.parseInt(l[2])]; // valid lines must have valid chromosome positions
    });

    JavaPairRDD<String, Integer> ones = valid.mapToPair(new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) {
            StringTokenizer line = new StringTokenizer(s, " \t");
            int chromosome1 = Integer.parseInt(line.nextToken());
            int position1 = Integer.parseInt(line.nextToken());
            int chromosome2 = Integer.parseInt(line.nextToken());
            int position2 = Integer.parseInt(line.nextToken());
            int bin1 = bins[chromosome1] + 1 + (int)Math.ceil(position1/100000); // calculate bin index
            int bin2 = bins[chromosome2] + 1 + (int)Math.ceil(position2/100000);
            if(bin1 > bin2){ // ensure bin1 less than bin2
                int temp = bin1;
                bin1 = bin2;
                bin2 = temp;
            }
            return new Tuple2<>("(" + bin1 + ", " + bin2 + ")", 1); // set count to 1 for bin pair
        }
    });


    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() { 
	public Integer call(Integer i1, Integer i2) {
	    return i1+i2;
	}
    });

    counts.saveAsTextFile("./output");
    spark.stop();
  }
}
