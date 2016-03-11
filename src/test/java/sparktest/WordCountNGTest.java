/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparktest;

import java.util.List;
import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import toolbox.random.Random;
import toolbox.util.MathUtil;

/**
 *
 * @author pabernathy
 */
public class WordCountNGTest {
    
    public WordCountNGTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    /**
     * Test of main method, of class WordCount.
     */
    @Test
    public void testMain() {
        System.out.println("TestNG main");
        String[] args = null;
        WordCount.main(args);
    }

    /**
     * Test of countWords method, of class WordCount.
     */
    @Test
    public void testCountWords() {
        System.out.println("TestNG countWords");
        WordCount.countWords();
    }

    /**
     * Test of setOperations method, of class WordCount.
     */
    @Test
    public void testSetOperations() {
        System.out.println("TestNG setOperations");
        WordCount.setOperations();
    }
	
	@Test
	public void testUseOutsideJar() {
		System.setProperty("spark.master", "http://localhost:4040");
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		List<Double> r = Random.rnormList(1000, 0, 1);
		JavaRDD<Double> rrdd = sc.parallelize(r);
		rrdd.foreach(n-> System.out.println(n));
		//assertEquals(1, MathUtil.sd(r));
		System.out.println(MathUtil.sd(r));
		assertEquals(MathUtil.sd(r), MathUtil.sd(rrdd.collect()));
        
        try {
            List<List> data = new ArrayList<List>();
            data.add(r);
            toolbox.io.CSVWriter.writeColumnsToFile(data, "randomnums.csv", "num", ",");
        } catch(java.io.IOException e) {
            System.err.println(e.getClass() + " " + e.getMessage());
        }
        
        JavaDoubleRDD drdd = rrdd.mapToDouble(d -> d);
        assertEquals(MathUtil.mean(r), drdd.mean(), .00001);
        //assertEquals(MathUtil.sd(r), drdd.stdev(), .000001);
        assertEquals(MathUtil.mean(r), drdd.mean(), .000001);
	}
    
}
