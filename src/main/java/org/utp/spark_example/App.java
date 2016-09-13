package org.utp.spark_example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * Simple example application for Apache Spark.
 * 
 * @author juanprq
 *
 */
public class App {
    public static void main( String[] args ) {
        String filePath = "resources/pg4014.txt";
        
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");;
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFileData = sc.textFile(filePath).cache();
        
        long numAs = textFileData.filter((line) -> {
            return line.contains("a");
        }).count();
        
        System.out.println("Número de lineas que contienen la letra a" + numAs);
        
        // primera línea
        String firstLine = textFileData.first();
        System.out.println("La primera línea es: " + firstLine);
        
        // contar cantidad de lineas
        long lineCount = textFileData.count();
        System.out.println("Cantidad de líneas de archivo: " + lineCount);
        
        // primera palabra
        JavaRDD<String> words = textFileData.flatMap((line) -> {
            List<String> wordList = Arrays.asList(line.split(" "));
            
            return wordList.iterator();
        });
        
        System.out.println("La primera palabra es: " + words.first());
        
        // contar candidad de palabras
        System.out.println("La cantidad de palabras encontradas es: " + words.count());
        
        // contar cada palabra
        JavaRDD<Tuple2<String, Integer>> counter = words.map((word) -> {
            return new Tuple2<String, Integer>(word, 1);
        });
        
        System.out.println("Primera tupla del conteo de palabras" + counter.first());
    }
}
