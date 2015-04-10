package fr.profi.mzdb.io.writer.mgf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


//import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.IsolationWindow;
import fr.profi.mzdb.db.model.params.Precursor;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.db.table.SpectrumTable;
import fr.profi.mzdb.io.reader.iterator.MsScanIterator;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.PeakEncoding;
import fr.profi.mzdb.model.Scan;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordOperation;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;

/**
 * @author MDB
 */
public class MgfWriter {

	public static String LINE_SPERATOR = System.getProperty("line.separator");

	private static Integer precNotFound = 0;

	final Logger logger = LoggerFactory.getLogger(MgfWriter.class);

	private static String titleQuery = "SELECT id, title FROM spectrum WHERE ms_level=2";

	private final String mzDBFilePath;

	private MzDbReader mzDbReader;

	private Map<Integer, String> titleByScanId = new HashMap<Integer, String>();

	private PrintWriter writer;

	/**
	 * 
	 * @param mzDBFilePath
	 * @throws SQLiteException
	 * @throws FileNotFoundException
	 */
	public MgfWriter(String mzDBFilePath) throws SQLiteException, FileNotFoundException {
		this.mzDBFilePath = mzDBFilePath;

		// Create reader
		try {
			this.mzDbReader = new MzDbReader(this.mzDBFilePath, true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SQLiteException e) {
			e.printStackTrace();
		}

		this._fillTitleByScanId();
		this.logger.info("Number of loaded spectra titles: " + this.titleByScanId.size());

	}

	private void _fillTitleByScanId() throws SQLiteException {

		/** inner class for treating sql resulting records */
		final class TitleByIdFiller implements ISQLiteRecordOperation {
			private Map<Integer, String> titleById;

			TitleByIdFiller(Map<Integer, String> t) {
				this.titleById = t;
			}

			@Override
			public void execute(SQLiteRecord elem, int idx) throws SQLiteException {
				int id = elem.columnInt(SpectrumTable.ID);
				String title = elem.columnString(SpectrumTable.TITLE);
				titleById.put(id, title);
			}
		} // end inner class

		TitleByIdFiller f = new TitleByIdFiller(this.titleByScanId);
		new SQLiteQuery(this.mzDbReader.getConnection(), titleQuery).forEachRecord(f);

	}

	/**
	 * 
	 * @param mgfFile
	 * @param pm
	 * @throws FileNotFoundException
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	public void write(String mgfFile, PrecursorMzComputation precComp// ,
	// float intensityCutoff
	) throws FileNotFoundException, SQLiteException, StreamCorruptedException {

		// treat path mgfFile ?
		if (mgfFile.isEmpty())
			mgfFile = this.mzDBFilePath + ".mgf";

		this.writer = new PrintWriter(new File("test.txt"));

		// iterate over ms2 scan
		final Iterator<Scan> scanIterator = new MsScanIterator(this.mzDbReader, 2);
		final PrintWriter mgfWriter = new PrintWriter(new File(mgfFile));
		final Map<Integer, DataEncoding> dataEncodingByScanId = this.mzDbReader.getDataEncodingByScanId();

		int spectraCount = 0;
		while (scanIterator.hasNext()) {
			Scan s = scanIterator.next();
			DataEncoding dataEnc = dataEncodingByScanId.get(s.getHeader().getId());
			String spectrumAsStr = this.stringifySpectrum(s, dataEnc, precComp); // , intensityCutoff);

			// make a space between two spectra
			mgfWriter.println(spectrumAsStr);
			mgfWriter.println();
			spectraCount++;
		}

		this.logger.info(String.format("MGF file successfully created: %d spectra exported.", spectraCount));
		this.logger.info(String.format("#Precursor not found: %d", MgfWriter.precNotFound));
		mgfWriter.flush();
		mgfWriter.close();

		this.writer.close();
	}

	/**
	 * 
	 * @param targetMz
	 * @return
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	public double refinePrecMz(double targetMz, double mzTolPPM, float time, Precursor precursor)
			throws SQLiteException, StreamCorruptedException {
		// do a XIC over isolation window
		final IsolationWindow iw = precursor.getIsolationWindow();
		final CVParam[] cvParams = iw.getCVParams(new CVEntry[] { CVEntry.ISOLATION_WINDOW_LOWER_OFFSET,
				CVEntry.ISOLATION_WINDOW_LOWER_OFFSET });
		final float lowerMzWindow = Float.parseFloat(cvParams[0].getValue());
		final float upperMzWindow = Float.parseFloat(cvParams[1].getValue());
		final double minmz = targetMz - lowerMzWindow;
		final double maxmz = targetMz + upperMzWindow;
		final double minrt = time - 10.0;
		final double maxrt = time + 10.0;
		final ScanSlice[] scanSlices = this.mzDbReader.getScanSlices(minmz, maxmz, minrt, maxrt, 1);
		final ArrayList<Peak> peaks = new ArrayList<Peak>();
		for (ScanSlice sl : scanSlices) {
			Peak p = sl.getData().getNearestPeak(targetMz, mzTolPPM);
			if (p != null) {
				p.setLcContext(sl.getHeader());
				peaks.add(p);
			}
		}
		// take the median value of mz
		if (peaks.isEmpty()) {
			MgfWriter.precNotFound++;
			/*
			 * this.logger.warn(lowerMzWindow +", " + upperMzWindow + ", " + targetMz);
			 * this.logger.warn("No peaks in XIC, that's sucks!");
			 */
			return targetMz;
		}

		if (peaks.size() == 1)
			return peaks.get(0).getMz();

		Collections.sort(peaks); // will use compareTo
		double medMz = 0.0;
		final int l = peaks.size();
		if (l % 2 != 0) {
			medMz = peaks.get(l / 2).getMz();
		} else {
			medMz = (peaks.get(l / 2 - 1).getMz() + peaks.get(l / 2).getMz()) / 2.0;
		}
		return medMz;
	}

	/**
	 * 
	 * @param scan
	 * @param dataEnc
	 * @param precComp
	 * @param intensityCutoff
	 * @return
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	protected String stringifySpectrum(Scan scan, DataEncoding dataEnc, PrecursorMzComputation precComp
	// float intensityCutoff
	) throws SQLiteException, StreamCorruptedException {

		String mzFragFormat = null;
		// FIXME: check if is_high_res parameter is used and is correct
		if (dataEnc.getPeakEncoding() == PeakEncoding.LOW_RES_PEAK) {
			mzFragFormat = "%.1f";
		} else { // We assume high resolution m/z for fragments
			mzFragFormat = "%.3f";
		}

		// unpack data
		final ScanHeader scanHeader = scan.getHeader();
		final String title = this.titleByScanId.get(scanHeader.getScanId());
		final float time = scanHeader.getElutionTime();
		double precMz = scanHeader.getPrecursorMz();

		if (precComp == PrecursorMzComputation.DEFAULT) {
			scanHeader.loadScanList(this.mzDbReader);
			UserParam precMzParam = scanHeader.getScanList().getScans().get(0)
					.getUserParam("[Thermo Trailer Extra]Monoisotopic M/Z:");

			double thermoTrailer = Double.parseDouble(precMzParam.getValue());

			scanHeader.loadPrecursorList(mzDbReader);
			Precursor precursor = scanHeader.getPrecursor();
			double refinedPrecMz = this.refinePrecMz(precMz, 20.0, time, precursor);
			if (Math.abs(refinedPrecMz - precMz) > 0.5) {
				System.out.println("" + precMz + ", " + refinedPrecMz + ", " + thermoTrailer);
			}

			if (Math.abs(refinedPrecMz - thermoTrailer) > 0.5) {
				System.out.println("" + precMz + ", " + refinedPrecMz + ", " + thermoTrailer);
			}

			this.writer.println(thermoTrailer - precMz);

		} else if (precComp == PrecursorMzComputation.REFINED_PWIZ) {
			try {
				scanHeader.loadScanList(this.mzDbReader);
				UserParam precMzParam = scanHeader.getScanList().getScans().get(0)
						.getUserParam("[Thermo Trailer Extra]Monoisotopic M/Z:");

				precMz = Double.parseDouble(precMzParam.getValue());
			} catch (NullPointerException e) {
				this.logger.trace("Refined pwiz user param name not found: fall back to default");
			}
		} else if (precComp == PrecursorMzComputation.REFINED_MZDB) {
			try {
				precMz = Double.parseDouble(scanHeader.getUserParam(
						PrecursorMzComputation.REFINED_MZDB.getUserParamName()).getValue());
			} catch (NullPointerException e) {
				this.logger.trace("Refined mdb user param name not found: fall back to default");
			}
			// if (precMz == 0.0d) precMz = scanHeader.getPrecursorMz();
		}

		final int charge = scanHeader.getPrecursorCharge();
		final MgfHeader mgfScanHeader = new MgfHeader(title, precMz, charge, time);

		StringBuilder spectrumStringBuilder = new StringBuilder();
		mgfScanHeader.appendToStringBuilder(spectrumStringBuilder);

		// Scan Data
		final ScanData data = scan.getData();
		final double[] mzs = data.getMzList();
		final float[] ints = data.getIntensityList();

		final int intsLength = ints.length;

		final double[] intsAsDouble = new double[intsLength];
		for (int i = 0; i < intsLength; ++i) {
			intsAsDouble[i] = (double) ints[i];
		}
		final double intensityCutOff = 0.0; // new Percentile().evaluate(intsAsDouble, 5.0);

		for (int i = 0; i < intsLength; ++i) {
			float intensity = ints[i];

			if (intensity >= intensityCutOff) {
				double mz = mzs[i];

				spectrumStringBuilder.append(String.format(mzFragFormat, mz)).append(" ")
						.append(String.format("%.0f", intensity)).append(LINE_SPERATOR);
			}
		}

		spectrumStringBuilder.append(MgfField.END_IONS);

		return spectrumStringBuilder.toString();
	}

}
