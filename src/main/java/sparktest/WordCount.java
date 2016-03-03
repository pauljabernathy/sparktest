package sparktest;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;

/**
 * Created by pabernathy on 2/21/16.
 */
public class WordCount
{
    private static SparkConf conf;
    private static JavaSparkContext sc;
    static {
        System.setProperty("spark.master", "http://localhost:4040");
        conf = new SparkConf().setAppName("wordCount");
        sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    }
    public WordCount() {
        /*System.setProperty("spark.master", "http://localhost:4040");
        conf = new SparkConf().setAppName("wordCount");
        sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        */
    }

	public static void main(String[] args)
	{
		//System.setProperty("spark.master", "http://localhost:4040");
		//SparkConf conf = new SparkConf().setAppName("wordCount");
		//JavaSparkContext sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		//WordCount wc = new WordCount();
        //wc.countWords();
        WordCount.countWords();
        /*String inputFile = "README.md";
		String outputFile = "output.txt";
		JavaRDD<String> input = sc.textFile(inputFile);
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>()
		{
			public Iterable<String> call(String x)
			{
				return Arrays.asList(x.split(" "));
			}
		});

		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>()
		{
			public Tuple2<String, Integer> call(String x)
			{
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			public Integer call(Integer x, Integer y)
			{
				return x + y;
			}
		});

		//counts.saveAsTextFile(outputFile);
		//System.out.println(counts);
		counts.foreach(count -> System.out.println(count));
		System.out.println(counts.count());

		JavaRDD<String> pythonLines = input.filter(line -> line.contains("Python"));
		System.out.println(pythonLines.count());
		//System.out.println(pythonLines);
		pythonLines.foreach(line -> System.out.println(line));
        */
	}
    
    public static void countWords() {
        /*System.setProperty("spark.master", "http://localhost:4040");
		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));*/
        String inputFile = "README.md";
		String outputFile = "output.txt";
		JavaRDD<String> input = sc.textFile(inputFile);
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>()
		{
			public Iterable<String> call(String x)
			{
				return Arrays.asList(x.split(" "));
			}
		});

		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>()
		{
			public Tuple2<String, Integer> call(String x)
			{
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			public Integer call(Integer x, Integer y)
			{
				return x + y;
			}
		});

		//counts.saveAsTextFile(outputFile);
		//System.out.println(counts);
        System.out.println(words.countByValue());
        
		counts.foreach(count -> System.out.println(count));
		System.out.println(counts.count());

		JavaRDD<String> pythonLines = input.filter(line -> line.contains("Python"));
		System.out.println(pythonLines.count());
		//System.out.println(pythonLines);
		pythonLines.foreach(line -> System.out.println(line));
    }
    public static void setOperations() {
        //X.foreach(System.out::println) is not working for whatever reason.
        JavaRDD<String> RDD1 = sc.parallelize(Arrays.asList("coffee", "coffee", "panda", "monkey", "tea"));
        JavaRDD<String> RDD2 = sc.parallelize(Arrays.asList("coffee", "monkey", "kitty"));
        System.out.println("RDD1.distinct()");
        JavaRDD<String> distinct = RDD1.distinct();
        distinct.foreach(line -> System.out.println(line));
        System.out.println("RDD2.distinct()");
        RDD2.distinct().foreach(item -> System.out.println(item));
        System.out.println("union");
        RDD1.union(RDD2).foreach(item -> System.out.println(item));
        System.out.println("intersection");
        RDD1.intersection(RDD2).foreach(item -> System.out.println(item));
        System.out.println("RDD1.subtract(RDD2)");
        RDD1.subtract(RDD2).foreach(item -> System.out.println(item));
        System.out.println("RDD2.subtract(RDD1)");
        RDD2.subtract(RDD1).foreach(item -> System.out.println(item));
        
        //take() and top() return a Java List, so we use forEach(),the Java8 function.  Sample returns a JavaRDD, so we use foreach(), the Spark function.
        /*System.out.println("");
        System.out.println("RDD1.take(2)");
        RDD1.take(2).forEach(item -> System.out.println(item));
        System.out.println("RDD1.top(2)");
        RDD1.top(2).forEach(item -> System.out.println(item));
        System.out.println("RDD1.sample(true, .5)");
        RDD1.sample(true, .5).foreach(item -> System.out.println(item));
        System.out.println("RDD1.sample(false, .5)");
        RDD1.sample(false, .5).foreach(item -> System.out.println(item));
                */
        
        List<Integer> numbersSource = new ArrayList<Integer>();
        int size = 100;
        for(int i = 0; i < size; i++) {
            numbersSource.add(i);
        }
        System.out.println(numbersSource.size());
        
        JavaRDD<Integer> numbers = sc.parallelize(numbersSource);
        System.out.println(numbers.count());
        System.out.println("numbers.take(10)");
        numbers.take(10).forEach(item -> System.out.println(item)); //0 through 9
        System.out.println("numbers.top(10)");
        numbers.top(10).forEach(item -> System.out.println(item));  //99 through 90
        System.out.println("numbers.sample(true, .2)");
        //numbers.sample(true, .2).foreach(item -> System.out.println(item));
        //Sort them to check uniqueness.  But I don't know the Spark sort method, so use Java 8.
        //This collect() is the spark function that returns a Java list.
        //numbers.sample(true, .2).collect().forEach(item -> System.out.println(item));
        List<Integer> sampleWithReplacement = numbers.sample(true, .2).collect();
        sampleWithReplacement.forEach(item -> System.out.println(item));
        System.out.println(sampleWithReplacement.size());
        if(sampleWithReplacement.size() != 20) {
            System.out.println("It was the wrong size.");
        } else {
            System.out.println("It was the correct size.");
        }
        System.out.println("numbers.sample(false, .2)");
        //numbers.sample(false, .2).foreach(item -> System.out.println(item));
        //numbers.sample(false, .2).collect().forEach(item -> System.out.println(item));
        List<Integer> sampleWithoutReplacement = numbers.sample(false, .2).collect();
        sampleWithoutReplacement.forEach(item -> System.out.println(item));
        System.out.println(sampleWithoutReplacement.size());
        if(sampleWithoutReplacement.size() != 20) {
            System.out.println("It was the wrong size.");
        } else {
            System.out.println("It was the correct size.");
        }
    }
}
