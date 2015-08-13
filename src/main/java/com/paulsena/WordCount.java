package com.paulsena;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.util.*;

/**
 * Apache Spark Word Counter
 *
 * Counts all occurrences of words from multiple books using Map Filter Reduce techniques.
 * Ability to run multiple instances of class and save/load word counts between program runs.
 * Requires Java 8 since it makes use of Lambda expressions, making the Functional-esq code a lot easier to read.
 * Also makes use of Java 8's Streams, and multiple exception catching features.
 *
 * @author Paul Senatillaka
 * @since 2015-07-24
 */
public class WordCount {

    //Main count data struct to share across all class instances.
    //Data Struct: [(Word,(Book, Count), ...]
    private static List<Tuple2<String,Tuple2<String, Integer>>> wordCounts = Collections.synchronizedList(new ArrayList<>());
    //Only one SparkContext instance allowed per JVM
    private static SparkConf sparkConf = new SparkConf().setAppName("EvergageWordCount").setMaster("local[4]");  //Runs cluster on 2 cores. Remove Local to run on multiple machines
    private static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    /**
     * Count all words in a given book in preparation for later querying.
     *
     * @param bookTitle The title of the book. Must be a non-empty String.
     * @param bookFile Path to the text file of the book. Must not be null.
     */
    public void countWords(String bookTitle, String bookFile) {

        List<Tuple2<String, Tuple2<String, Integer>>> processed = sparkContext.textFile(bookFile)
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .map(word -> word.toLowerCase().replaceAll("^[^a-z]+|[^a-z]+$", "").trim())  // Removes all non letter chars from beginning and end of word
                .filter(word -> !word.isEmpty()) // Remove Empty Strings
                .mapToPair(word -> new Tuple2<>(word, 1)) // Create simple (Word, 1) Pairs
                .reduceByKey((a, b) -> a + b) // Accumulate counts
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), new Tuple2<>(bookTitle, tuple._2()))) // Create composite KV Pairs of: (Word,(Book, Count))
                .collect();

        wordCounts.addAll(processed); //Store in static List on driver node.
    }


    /**
     * Provides the top ten most common words found cumulatively across all
     * books that have been processed by the service.
     *
     * @return A mapping of at most ten words to their respective counts. Never
     *         null, but possibly empty.
     */
    public Map<String, Integer> topTenWords() {

        Map<String,Integer> results = new HashMap<>();

        //Don't really need to use Spark here since word totals are already stored on driver node, could just do a local
        //filter and sort using Java Streams. However for sake of learning, will parallelize with Spark.
        sparkContext.parallelize(wordCounts)
                .mapToPair(t -> new Tuple2<>(t._1(), t._2()._2())) // (Word, Count)
                .reduceByKey((a, b) -> a + b)  // Accumulate all word counts
                .top(10, new WordCount.ValueComparator())  // Get top 10 sorted by Count value
                .forEach(t -> results.put(t._1(), t._2()));

        return results;
    }

    /**
     * Provides the top ten most common words found in a book that was already
     * processed by the service.
     *
     * @param bookTitle The title of the book. Must be a non-empty String.
     * @return A mapping of at most ten words to their respective counts. Never
     *         null, but possibly empty.
     */
    public Map<String, Integer> topTenWords(String bookTitle) {

        Map<String,Integer> results = new HashMap<>();

        //Don't really need to use Spark here since word totals are already stored on driver node, could just do a local
        //filter and sort using Java Streams. However for sake of learning, will parallelize with Spark.
        sparkContext.parallelize(wordCounts)
                .filter(t -> (t._2()._1().contentEquals(bookTitle))) // Filter just for Book
                .mapToPair(t -> new Tuple2<>(t._1(), t._2()._2()))  // (Word, Count)
                .top(10, new WordCount.ValueComparator())  // Get top 10 sorted by Count value
                .forEach(t -> results.put(t._1(), t._2()));

        return results;
    }

    /**
     * Saves the current count of all words that have been processed.
     * Use this in between program runs to keep history of counts.
     *
     * @param fileName Filename of the cache file. Can't be empty.
     * @return Boolean of if the save operation worked.
     */
    public boolean saveCounts(String fileName) {

        try {
            FileOutputStream fStream = new FileOutputStream(fileName);
            ObjectOutputStream oStream = new ObjectOutputStream(fStream);
            oStream.writeObject(wordCounts);
            oStream.close();
        }
        catch (IOException ex) {
            System.out.println(ex);
            return false;
        }
        return true;
    }

    /**
     * Loads count of all words that have been processed. Replaces current counts in memory, so use before processing.
     * Use this in between program runs to keep history of counts.
     *
     * @param fileName Filename of the cache file. Can't be empty.
     * @return Boolean of if the save operation worked.
     */
    public boolean loadCounts(String fileName) {
        try {
            FileInputStream fStream = new FileInputStream(fileName);
            ObjectInputStream sStream = new ObjectInputStream(fStream);
            wordCounts = (List<Tuple2<String,Tuple2<String, Integer>>>) sStream.readObject();
            sStream.close();
        }
        catch (IOException|ClassNotFoundException ex) {
            System.out.println(ex);
            return false;
        }
        return true;
    }

    /**
     * Helper function to log current counts of all processed words.
     */
    public void logCounts() {
        wordCounts.forEach(tuple -> System.out.println(tuple));
    }

    /**
     * Helper function to log current counts passed in Map.
     * @param map Map<String,Integer> object to log
     */
    public void logMap(Map<String, Integer> map) {
        map.forEach((K,V) -> System.out.println(K + " : " + V));
    }

    /**
     * Method to stop the static final Apache Spark instance. Must be called by at least one WordCount instance.
     * There can only be one Spark context running on the JVM at a time.
     */
    public void stopSpark() {
            sparkContext.stop();
    }

    /**
     * Custom comparator for comparing KV values instead of keys in Spark Sort operations
     */
    static class ValueComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
            return tuple1._2().compareTo(tuple2._2());
        }
    }

}
