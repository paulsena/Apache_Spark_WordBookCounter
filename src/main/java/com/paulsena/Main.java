package com.paulsena;


import scala.Console;

public class Main {

    public static void main(String[] args) {

        WordCount wordCount = new WordCount();
        WordCount wordCount2 = new WordCount();

        //Loads previous runs word count cache
        //wordCount.loadCounts("WordCounts.sav");

        wordCount.countWords("Alice", "Alice In Wonderland.txt");
        wordCount.countWords("Moby Dick", "Moby Dick.txt");

        //wordCount2.logCounts();

        System.out.println("Top Ten Words - All Books:");
        wordCount2.logMap(wordCount.topTenWords());

        System.out.println("Top Ten Words for Moby Dick:");
        wordCount2.logMap(wordCount.topTenWords("Moby Dick"));

        wordCount.saveCounts("WordCounts.sav");
        wordCount.stopSpark();
    }
}
