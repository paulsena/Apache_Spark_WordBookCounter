package com.paulsena;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import java.util.Map;

import static org.junit.Assert.*;


public class WordCountTest {
    static WordCount wordCount;
    static WordCount wordCount2;

    @BeforeClass
    public static void setUp() throws Exception {
        //Run this only once per test run b/c there can only be one instance of Spark running at a time.
        wordCount = new WordCount();
        wordCount2 = new WordCount();
        //Since counts are persisted across instances, do this once.
        //Also loading into instance 1 and tests will use instance 2 to show global count working.
        wordCount.countWords("Test Book 1", "TestBook1.txt");
        wordCount.countWords("Test Book 2", "TestBook2.txt");
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Before
    public void setupData() {
    }

    @Test
    public void testTopTenWords() throws Exception {
        //Querying difference instance of class then where words were processed.
        Map<String, Integer> result = wordCount2.topTenWords();

        assertNotNull(result);
        assertTrue(result.get("hello") == 11);
        assertTrue(result.get("world") == 11);
        assertTrue(result.size() == 10);
    }

    @Test
    public void testTopTenWordsBook() throws Exception {
        Map<String, Integer> result = wordCount2.topTenWords("Test Book 2");

        assertNotNull(result);
        assertTrue(result.get("this") == 1);
        assertTrue(result.get("is") == 1);
        assertTrue(result.get("a") == 1);
        assertTrue(result.size() == 10);
    }

}