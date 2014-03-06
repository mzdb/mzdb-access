package fr.profi.mzdb.io.writer;

//import fr.profi.mzdb.model.Scan;
import fr.profi.mzdb.model.ScanHeader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.FileOutputStream;

import org.apache.commons.lang3.StringUtils;

//import com.codahale.jerkson.Json.generate;

public class MsScanTSVWriter {

	public static void writeRun(ScanHeader[] scans, Integer runId, File outFile) {

		// implicit def string2File(filename: String) = new File(filename)

		PrintWriter out = null;
		try {
			out = new PrintWriter(new FileOutputStream(outFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		String[] colnames = { "id", "initial_id", "cycle", "time", "ms_level", "tic", "base_peak_moz",
				"base_peak_intensity", "precursor_moz", "precursor_charge", "serialized_properties", "run_id" };
		out.print(StringUtils.join(colnames, "\t") + "\n");

		for (ScanHeader scan : scans) {
			out.print(scan2String(scan, runId));
		}
		// out.flush()
		out.close();

	}

	private static String scan2String(ScanHeader scan, Integer runId) {
		// ScanHeader header = scan.getHeader();

		String[] scanValues = { String.valueOf(scan.getId()), String.valueOf(scan.getInitialId()),
				String.valueOf(scan.getCycle()), String.valueOf(scan.getTime()),
				String.valueOf(scan.getMsLevel()), String.valueOf(scan.getTIC()),
				String.valueOf(scan.getBasePeakMz()), String.valueOf(scan.getBasePeakIntensity()),
				String.valueOf(scan.getPrecursorMz()), String.valueOf(scan.getPrecursorCharge()), "",// generate(LcmsScanProperties)
				String.valueOf(runId) };
		return StringUtils.join(scanValues, "\t") + "\n";
	}
}