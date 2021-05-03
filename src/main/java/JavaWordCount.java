/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.apache.spark.examples;
import org.apache.spark.SparkConf;
//import  org.apache.spark.SparkContext.getConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

//import org.apache.spark.sql.DataFrameReader;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Iterator;

import org.apache.log4j.*;


public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");
  static Logger logger; 
  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWordCount")
      .getOrCreate();
      // initialize log4j
      BasicConfigurator.configure();
      logger = Logger.getLogger(JavaWordCount.class.getName());
      System.out.println("My config sina is here: " + spark.conf().getAll().toString() );
      logger.trace("printing a sample line  000 \n");

 

     ////JavaRDD<String> data = jsc.textFile("hdfs://192.168.210.11:8020/input_wordcount");

    JavaRDD<String> lines = spark.read().textFile("hdfs://192.168.210.11:8020/input_wordcount").javaRDD();
      //JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();// textFile(args[0]).cache();
      System.err.flush();
      System.out.flush();    
	long startTime = System.currentTimeMillis();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    List<Tuple2<String, Integer>> output = counts.collect();
   //for (Tuple2<?,?> tuple : output) {
    //  System.out.println(tuple._1() + ": " + tuple._2());
    //}
    counts.saveAsTextFile("hdfs://192.168.210.11:8020/output");  //directory will be on the desktop!!!
    //counts.saveAsTextFile("output.txt");
 
      //for (int i=0; i<1000000; i++)
      //{
      //  logger.trace("printing a sample line"+ i + "\n");
      //}
      System.err.flush();
      System.out.flush();
	    long endTime   = System.currentTimeMillis();
      long totalTime = endTime - startTime;
      
      spark.stop();
      System.err.flush();
      System.out.flush();
      System.out.println("Total run-time was: "+ totalTime);
      //logger.trace("printing a sample line\n");
  }
}
