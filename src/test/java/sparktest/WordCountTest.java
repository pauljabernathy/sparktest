/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparktest;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author pabernathy
 */
public class WordCountTest {
    
    public WordCountTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class WordCount.
     */
    @Test
    public void testMain() {
        System.out.println("JUnit main");
        String[] args = null;
        //WordCount.main(args);
    }

    /**
     * Test of countWords method, of class WordCount.
     */
    @Test
    public void testCountWords() {
        System.out.println("JUnit countWords");
        WordCount.countWords();
    }

    /**
     * Test of setOperations method, of class WordCount.
     */
    @Test
    public void testSetOperations() {
        System.out.println("JUnit setOperations");
        WordCount instance = new WordCount();
        instance.setOperations();
    }
    
}
