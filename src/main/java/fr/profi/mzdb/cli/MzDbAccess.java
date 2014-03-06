package fr.profi.mzdb.cli;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.commons.cli.*;
import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.model.*;
import fr.profi.mzdb.MzDbReader;

/***
 * This class allows to access to a mzDB file and to make some range queries on it. A list of putative
 * features can be provided to extract their corresponding signal.
 * 
 * @version 0.1
 * 
 * @author David Bouyssié
 * 
 */
public class MzDbAccess {

	/**
	 * Print a string in the standard output and terminate the line. Works only if mode<-PRINT_MODE.
	 * 
	 * @param string
	 *            the string to print
	 * @param mode
	 *            can only assume values PRINT_ALWAYS or PRINT_DEBUG.
	 * */
	protected static void println(String string) {
		System.out.println(string);
	}

	/**
	 * Print a string in the standard output. Works only if mode<-PRINT_MODE.
	 * 
	 * @param string
	 *            the string to print
	 * @param mode
	 *            can only assume values PRINT_ALWAYS or PRINT_DEBUG.
	 */
	protected static void print(String string) {
		System.out.print(string);
	}

	protected static Options getOptions() {

		// Define command line options
		Options tmpOptions = new Options();
		tmpOptions.addOption("file", true, "the file path to the mzDB");
		tmpOptions.addOption("min_mz", true, "minimum m/z value");
		tmpOptions.addOption("max_mz", true, "maximum m/z value");
		tmpOptions.addOption("min_time", true, "minimum elution time");
		tmpOptions.addOption("max_time", true, "maximum elution time");

		Options options = new Options();

		// iterate over the options to set them required
		Object[] optList = tmpOptions.getOptions().toArray();
		for (int i = 0; i < optList.length; i++) {
			// get the next Option
			Option option = (Option) optList[i];
			option.setRequired(true);
			options.addOption(option);
		}

		// Add non required options
		// options.addOption("charge", true, "feature charge");
		// options.addOption("peakels_count", true,
		// "number of feature eluting peaks");

		return options;
	}

	public static int _binSearchIndexToNearestIndex(int binSearchIndex, int length) {
		if (binSearchIndex >= 0)
			return binSearchIndex;
		else {
			int idx = -binSearchIndex - 1;
			if (idx == 0)
				return -1;
			else
				return idx;
		}
	}

	/**
	 * @param args
	 * @throws SQLiteException
	 */
	public static void main(String[] args) throws SQLiteException {

		// Retrieve command line options
		Options options = getOptions();

		// Parse command line options
		CommandLineParser parser = new GnuParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(options, args);
		} catch (ParseException pe) {

			// Display error message
			System.err.println("Parsing command line arguments failed: " + pe.getMessage());

			// Display help message
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("options to provide are", options);

			return;
		}

		double min_mz = Double.parseDouble(cmdLine.getOptionValue("min_mz"));
		double max_mz = Double.parseDouble(cmdLine.getOptionValue("max_mz"));
		int min_time = Integer.parseInt(cmdLine.getOptionValue("min_time"));
		int max_time = Integer.parseInt(cmdLine.getOptionValue("max_time"));

		System.out.println("Running mzDBaccess with following parameters :");
		System.out.println("- min_mz=" + min_mz);
		System.out.println("- max_mz=" + max_mz);
		System.out.println("- min_time=" + min_time);
		System.out.println("- max_time=" + max_time);

		// String dbPath = "F:/LCMS/huvec/mzdb/OENYD100205_05.raw.mzDB.sqlite";
		String dbPath = cmdLine.getOptionValue("file");
		println("accessing to mzDB located at " + dbPath);

		/*
		 * double[] values = {10.0,40.0,50.0}; int minBinSearchIndex = Arrays.binarySearch(values, 9.0); int
		 * minIdx = _binSearchIndexToNearestIndex(minBinSearchIndex,values.length); System.out.println( minIdx
		 * ); Object t = null; System.out.println( t.hashCode() );
		 */

		// Instantiate the mzDB
		MzDbReader mzDbInstance = null;
		try {
			mzDbInstance = new MzDbReader(new File(dbPath), true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Retrieve peaks
		try {

			Peak[] peaks = mzDbInstance.getPeaks(min_mz, max_mz, min_time, max_time, 1);

			if (peaks != null) {

				for (Peak peak : peaks) {
					println(peak.getMz() + "\t" + peak.getIntensity() + "\t" + peak.getLeftHwhm() + "\t"
							+ peak.getRightHwhm());

				}

				// println(peaks.toString());
				// println( "peaks count: " + peaks.size() );
			}

		} catch (SQLiteException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		mzDbInstance.close();

	}

}
