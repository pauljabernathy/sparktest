/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparktest;

import static org.testng.Assert.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    
}
