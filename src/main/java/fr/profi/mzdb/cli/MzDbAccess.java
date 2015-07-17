package fr.profi.mzdb.cli;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.io.writer.mgf.MgfWriter;
import fr.profi.mzdb.io.writer.mgf.PrecursorMzComputation;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.ScanHeader;

/***
 * This class allows to access to a mzDB file and to make some range queries on it. A list of putative
 * features can be provided to extract their corresponding signal.
 * 
 * @version 0.1
 * 
 * @author David Bouyssie
 * 
 */
public class MzDbAccess {
	
	static final Logger logger = LoggerFactory.getLogger(MzDbAccess.class);

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

	public static class ExtractPeaksCommand {
		@Parameter
		private List<String> parameters = new ArrayList<String>();

		@Parameter(names = { "-mzdb", "--mzdb_file_path" }, description = "mzDB file to perform extraction", required = true)
		private String mzdbFile = "";

		@Parameter(names = { "-mz1", "--minmz" }, description = "minimum m/z value", required = true)
		private Double minMz = 0.0;

		@Parameter(names = { "-mz2", "--maxmz" }, description = "maximum m/z value", required = true)
		private Double maxMz = 0.0;

		@Parameter(names = { "-t1", "--mintime" }, description = "minimum elution time")
		private Double minTime = 0.0;

		@Parameter(names = { "-t2", "--maxtime" }, description = "maximum elution time")
		private Double maxTime = 0.0;
	}

	public static class CreateMgfCommand {

		@Parameter
		private List<String> parameters = new ArrayList<String>();

		@Parameter(names = { "-mzdb", "--mzdb_file_path" }, description = "mzDB file to perform extraction", required = true)
		private String mzdbFile = "";

		@Parameter(names = { "-o", "--output_file_path" }, description = "mgf output file path", required = true)
		private String outputFile = "";

		@Parameter(names = { "-precmz", "--precursor_mz" }, description = "must be on of 'main_precursor_mz, selected_ion_mz, extracted, refined, refined_thermo'", required = false)
		private PrecursorMzComputation precMzComputation = PrecursorMzComputation.MAIN_PRECURSOR_MZ;
		
		@Parameter(names = { "-mztol", "--mz_tol_ppm" }, description = "m/z tolerance used for precursor m/z value definition", required = false)
		private float mzTolPPM = 20;

		@Parameter(names = { "-cutoff", "--intensity_cutoff" }, description = "optional intensity cutoff to use", required = false)
		private float intensityCutoff = 0f;
		
		@Parameter(names = { "-ptitle", "--proline_title" }, description = "export TITLE using the Proline convention", required = false)
		private boolean exportProlineTitle = false;
	}

	public static class DebugCommand {

		@Parameter
		private List<String> parameters = new ArrayList<String>();

		@Parameter(names = { "-mzdb", "--mzdb_file_path" }, description = "mzDB file to perform extraction", required = true)
		private String mzdbFile = "";
	}

	/**
	 * @param args
	 * @throws SQLiteException
	 */
	public static void main(String[] args) throws SQLiteException {

		Locale englishLocale = Locale.ENGLISH;
		Locale.setDefault(englishLocale);

		JCommander jc = new JCommander();
		ExtractPeaksCommand xicCmd = new MzDbAccess.ExtractPeaksCommand();
		CreateMgfCommand mgfCmd = new MzDbAccess.CreateMgfCommand();
		DebugCommand dbgCmd = new MzDbAccess.DebugCommand();
		jc.addCommand("extract_peaks", xicCmd);
		jc.addCommand("create_mgf", mgfCmd);
		jc.addCommand("debug", dbgCmd);

		try {
			jc.parse(args);

			String parsedCommand = jc.getParsedCommand();
			if (parsedCommand == null || parsedCommand == "") {
				println("No command provided. Exiting");
				printAvailableCommands(jc);
				System.exit(1);
			}
			if (parsedCommand.equals("extract_peaks")) {
				extractPeaks(xicCmd);
			} else if (parsedCommand.equals("create_mgf")) {
				createMgf(mgfCmd);
			} else if (parsedCommand.equals("debug")) {
				debug(dbgCmd);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void printAvailableCommands(JCommander jc) {
		println("Available commands:");
		for (JCommander e : jc.getCommands().values()) {
			e.usage();
		}
	}

	private static Peak[] extractPeaks(ExtractPeaksCommand epc) {
		String dbPath = epc.mzdbFile;
		double minMz = epc.minMz;
		double maxMz = epc.maxMz;
		double minTime = epc.minTime;
		double maxTime = epc.maxTime;

		System.out.println("Running mzDBaccess with following parameters :");
		System.out.println("- min_mz=" + minMz);
		System.out.println("- max_mz=" + maxMz);
		System.out.println("- min_time=" + minTime);
		System.out.println("- max_time=" + maxTime);

		// String dbPath = "F:/LCMS/huvec/mzdb/OENYD100205_05.raw.mzDB.sqlite";
		println("accessing to mzDB located at " + dbPath);

		// Instantiate the mzDB
		MzDbReader mzDbInstance = null;
		try {
			mzDbInstance = new MzDbReader(new File(dbPath), true);
		} catch (SQLiteException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// Retrieve peaks
		try {
			Peak[] peaks = mzDbInstance.getPeaks(minMz, maxMz, minTime, maxTime, 1);
			if (peaks != null) {
				for (Peak peak : peaks) {
					println(peak.getMz() + "\t" + peak.getIntensity() + "\t" + peak.getLeftHwhm() + "\t"
							+ peak.getRightHwhm());
				}
			}
			return peaks;
		} catch (SQLiteException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mzDbInstance.close();
		return null;
	}

	private static void createMgf(CreateMgfCommand cmd) throws SQLiteException, IOException, ClassNotFoundException {
		
		logger.info("Creating MGF File for mzDB at: " + cmd.mzdbFile);
		logger.info("Precursor m/z values will be defined using the method: " + cmd.precMzComputation);

		MgfWriter writer = new MgfWriter(cmd.mzdbFile);
		writer.write(cmd.outputFile, cmd.precMzComputation, cmd.mzTolPPM, cmd.intensityCutoff, cmd.exportProlineTitle);
	}

	private static void debug(DebugCommand cmd) throws SQLiteException, FileNotFoundException {

		MzDbReader mzDbReader = null;
		try {
			mzDbReader = new MzDbReader(cmd.mzdbFile, true);
			ScanHeader[] ms2ScanHeaders = mzDbReader.getMs2ScanHeaders();

			for (ScanHeader ms2ScanHeader: ms2ScanHeaders) {
				ms2ScanHeader.loadScanList(mzDbReader);
				ms2ScanHeader.getParamTree(mzDbReader);

				UserParam precMzParam = ms2ScanHeader.getScanList().getScans().get(0)
						.getUserParam("[Thermo Trailer Extra]Monoisotopic M/Z:");

				// <userParam name="[Thermo Trailer Extra]Monoisotopic M/Z:" value="815.21484375"
				// type="xsd:float"/>
				System.out.println(precMzParam.getValue());

				// .getUserParams().get(0).getValue()
				break;
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SQLiteException e) {
			e.printStackTrace();
		} finally {
			if (mzDbReader != null)
				mzDbReader.close();
		}

	}

}
