import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple3;

/**
 * Part 1 - Extract the information we want from the file
 * 
 * Use an accumulator to count all the valid and invalid records in the file for the S&P 500
 * 
 * List the <Symbol, Dividend Yield, Price/Earning> information as output.
 * 
 * Make sure you filter out anything that is not a valid entry
 * 
 * ---------------------------------------
 * 
 * Input: companies/SP500-constituents-financials.csv
 * 
 * **Look at the file**, the first line is a header, listing the information in each column
 * 
 * Output a list containing a Tuple3 of <Symbol, Dividend Yield, Price/Earnings>
 * 
 * Output the number of valid records in the file - is it 500?
 * 
 * ----------------------------------------
 * 
 * @author Aidyn Kemeldinov
 *
 */
public class Spark_Part1{

	private final static String recordRegex = ",";
	private final static Pattern REGEX = Pattern.compile(recordRegex);

	// Initialize index parsing values 
	private static final int SYMBOL_INDEX = 0;
	private static final int DIVIDEND_INDEX = 4;
	private static final int EARNING_INDEX = 5;
	
	//initialize a String for representing the header and negative infinity
	private final static String HEADER = "Symbol";
	private final static float N_INFINITY = Float.NEGATIVE_INFINITY;
	/*
	 * You may want a testing flag so you can switch off debugging information easily
	 */
	private static boolean testing = true;

	/**
	 * In main I have supplied basic information for starting the job in Eclipse.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out.println("Arguments provided:  ");
			for (String arg : args) {
				System.out.println(arg);
			}
			System.out.printf("Usage: Provide <input dir> <output dir> \n");
			System.out.printf("Example: data/companies/SP500-constituents-financials.csv output/output_1 \n");
			System.exit(-1);
		}

		/*
		 * Setup job configuration and context
		 */
		SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.setMaster("local");
		conf.setAppName("Part 1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/*
		 * Setup output
		 */
		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();
		/*
		 * Setup accumulators for counting records - 
		 * numberRecord	- 	Number of record lines
		 * badRecord	- 	Total number of blank records
		 * blankDiv		-	Number of blank Dividend records
		 * blankEarn	-	Number of blank Earning/Price records
		 * invalidRecord-	Number of invalid records
		 */
		final Accumulator<Integer> numberRecord = sc.accumulator(0);
		final Accumulator<Integer> badRecord = sc.accumulator(0);
		final Accumulator<Integer> blankDiv = sc.accumulator(0);
		final Accumulator<Integer> blankEarn = sc.accumulator(0);
		final Accumulator<Integer> invalidRecord = sc.accumulator(0);
		/*
		 * Read the lines in a file
		 */
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		/*
		 * if testing, cache the file and then check out the first ten lines in the file
		 */
		if (testing) {
			/*
			 * Show the header - notice, it is the first line in the file, so we can use first() - an action.
			 * 
			 * Data that is not cached is discarded after an action, so we should cache here or Spark will read the file
			 * again to recreate lines for the next action.
			 */
			lines.cache();
			System.out.println("Header found "+lines.first().contains(HEADER)+" Index is "+lines.first().indexOf(HEADER));
		}
		
		
		// Map to parse each line to a Tuple3 containing <Symbol, Dividend Yield, Price/Earnings>
		JavaRDD<Tuple3<String, String, String>> filterInfo = lines.map(line -> line.split(recordRegex)).
				map(line -> new Tuple3<>(line[SYMBOL_INDEX],line[DIVIDEND_INDEX],line[EARNING_INDEX])).
				// Remove header line
				filter(line -> !(line._1().contains(HEADER)));
		
		//Count number of records
		filterInfo.foreach(line -> numberRecord.add(1));
		JavaRDD<Tuple3<String, String, String>> filteredInfo = filterInfo.map(line -> {
				if(line._2().isEmpty()) {
					badRecord.add(1); // Increment total number of record
					blankDiv.add(1);  // Increment number blank DIVIDEND
					return new Tuple3<>(line._1(),"0.0",line._3());// replace blank value with 0.0
				}
				else return line;}).
			map(line -> {
				if(line._3().isEmpty()) {
					badRecord.add(1);
					blankEarn.add(1);
					return new Tuple3<>(line._1(),line._2(),Float.toString(N_INFINITY));
				}
				else return line;		
			});
		
		// Sort list alphabetically
		JavaRDD<Tuple3<String, String, String>> sortInfo= filteredInfo.sortBy((line-> line._1()), true, 1);
		
		sortInfo.foreach(x -> {
			try {
				Float.parseFloat(x._2());
				Float.parseFloat(x._3());
			} catch (NumberFormatException e) {
				invalidRecord.add(1);;
			}
		});

		// Write information out to a file using "saveAsTextFile"
		sortInfo.saveAsTextFile(outputPath);
		
		// Print the accumulator information to the console
		System.out.println("Number of records: " + numberRecord.value());
		System.out.println("Number of bad values: "+badRecord.value());
		System.out.println("Where bad dividends: "+blankDiv.value()+" and bad price/earnings: "+blankEarn.value());
		System.out.println("Number of invalid records: "+invalidRecord.value());
		sc.close();
	}
}
