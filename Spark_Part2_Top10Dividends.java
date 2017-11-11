import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

public class Spark_Part2_Top10Dividends {

	public static class ParseLine implements PairFunction<String, String, String[]> {
		@Override
		public Tuple2<String, String[]> call(String line) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] elements = reader.readNext();
			String key = elements[0];
			return new Tuple2<String, String[]>(key, elements);
		}
	}

	static class DividendComparator implements Comparator<Tuple2<String, Float>>, Serializable {

		final static DividendComparator INSTANCE = new DividendComparator();

		@Override
		public int compare(Tuple2<String, Float> kv1, Tuple2<String, Float> kv2) {

			Float value1 = kv1._2();
			Float value2 = kv2._2();

			return -value1.compareTo(value2); // sort descending

			// return value1[0].compareTo(value2[0]); // sort ascending
		}
	}
	
	private static final String HEADER = "Symbol";
	private static final int DIVIDEND_INDEX = 4;
	private static final int FIFTY_INDEX = 9;

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception("Arguments for Eclipse: <csvFile> <regexFile>");
		}
		String csv = args[0];
		String csvWithQuotes = args[1];
		Spark_Part2_Top10Dividends app = new Spark_Part2_Top10Dividends();
		app.run(csv, csvWithQuotes);
	}

	public void run(String csvFile, String csvWithQuotesFile) throws Exception {

		SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.setMaster("local");
		conf.setAppName("Spark_Part2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> csvRecords = sc.textFile(csvFile);
		JavaRDD<String> csvWithQuotesRecords = sc.textFile(csvWithQuotesFile);
		JavaPairRDD<String, String[]> keyedRDD1 = csvRecords.mapToPair(new ParseLine());
		JavaPairRDD<String, String[]> keyedRDD2 = csvWithQuotesRecords.mapToPair(new ParseLine());

		
		JavaPairRDD<String, Tuple2<String[], String[]>> joinResults = keyedRDD2.join(keyedRDD1).
														//Discard header from "table"
														filter(line -> !(line._1().contains(HEADER))).
														//Sort by key alphabetically
														sortByKey(true,1);
		joinResults.cache();
		
		// Map to pair (KEY, DIVIDEND VALUE)
		JavaPairRDD<String, Float> dividends = joinResults.mapValues(x -> {
			try {
				return Float.valueOf(x._1()[DIVIDEND_INDEX]);
			} catch (NumberFormatException e) {
				return 0f;
			}
		});
		
		// Map to pair (KEY, 52 HIGH)
		JavaPairRDD<String, Float> fiftyTwo = joinResults.mapValues(x -> {
			try {
				return Float.valueOf(x._1()[FIFTY_INDEX]);
			} catch (NumberFormatException e) {
				return 0f;
			}
		});

		List<Tuple2<String, Float>> top10Dividends = dividends.takeOrdered(10, DividendComparator.INSTANCE);
		List<Tuple2<String, Float>> top10fifty = fiftyTwo.takeOrdered(10, DividendComparator.INSTANCE);
		
		//Print list of joined "table"
		joinResults.foreach(line -> {
			//String [] word = line._2();
			System.out.println("Stock-name= "+line._1+
					" stock-info1= "+Arrays.toString(line._2()._1())+" stock-info2= "+
					Arrays.toString(line._2()._2()));
		});
		
		// Answer to how many stocks are both on the NASDAQ and in the SP500
		System.out.println("Number of records on both the NASDAQ and the SP500:  " + joinResults.count());
		
		// Answer to what are the top 10 stocks with the highest dividends
		System.out.println("\nTop 10 stocks with the highest dividends:");
		for (Tuple2<String, Float> element : top10Dividends)
			System.out.println("\t"+element._1 + "," + element._2);
		
		// Answer to what are the top 10 stocks with the greatest percentage increase over a 52-week period
		System.out.println("\nTop 10 stocks with the greatest percentage increase over a 52-week period: ");
		for (Tuple2<String, Float> element : top10fifty)
			System.out.println("\t"+element._1 + "," + element._2);
		
	}
}
