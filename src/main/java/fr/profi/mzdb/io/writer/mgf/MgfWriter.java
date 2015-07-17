package fr.profi.mzdb.io.writer.mgf;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
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
import fr.profi.mzdb.db.model.params.IsolationWindowParamTree;
import fr.profi.mzdb.db.model.params.Precursor;
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.db.table.SpectrumTable;
import fr.profi.mzdb.io.reader.ScanHeaderReader;
import fr.profi.mzdb.io.reader.iterator.MsScanIterator;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.PeakEncoding;
import fr.profi.mzdb.model.Scan;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.ms.MsUtils;
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

	/**
	 * 
	 * @param mzDBFilePath
	 * @throws SQLiteException
	 * @throws FileNotFoundException
	 * @throws ClassNotFoundException 
	 */
	public MgfWriter(String mzDBFilePath) throws SQLiteException, FileNotFoundException, ClassNotFoundException {
		this.mzDBFilePath = mzDBFilePath;

		// Create reader
		this.mzDbReader = new MzDbReader(this.mzDBFilePath, true);

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
	 * @throws SQLiteException
	 * @throws IOException 
	 */
	public void write(String mgfFile, PrecursorMzComputation precComp, float mzTolPPM, float intensityCutoff, boolean exportProlineTitle ) throws SQLiteException, IOException {

		// treat path mgfFile ?
		if (mgfFile.isEmpty())
			mgfFile = this.mzDBFilePath + ".mgf";
		
		// Reset precNotFound static var
		MgfWriter.precNotFound = 0;
		
		// Configure the ScanHeaderReader in order to load all precursor lists when reading spectra headers
		ScanHeaderReader.loadPrecursorList = true;

		// Iterate over MS2 scan
		final Iterator<Scan> scanIterator = new MsScanIterator(this.mzDbReader, 2);
		final PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(mgfFile)));
		final Map<Long, DataEncoding> dataEncodingByScanId = this.mzDbReader.getDataEncodingByScanId();

		int spectraCount = 0;
		while (scanIterator.hasNext()) {
			
			Scan s = scanIterator.next();
			long scanId = s.getHeader().getId();
			DataEncoding dataEnc = dataEncodingByScanId.get(scanId);
			String spectrumAsStr = this.stringifySpectrum(s, dataEnc, precComp, mzTolPPM, intensityCutoff, exportProlineTitle);
			
			//this.logger.debug("Writing spectrum with ID="+scanId);

			// Write the spectrum			
			mgfWriter.println(spectrumAsStr);
			
			// Write a blank line between two spectra
			mgfWriter.println();
			
			spectraCount++;
		}

		this.logger.info(String.format("MGF file successfully created: %d spectra exported.", spectraCount));
		this.logger.info(String.format("#Precursor not found: %d", MgfWriter.precNotFound));
		mgfWriter.flush();
		mgfWriter.close();
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
	protected String stringifySpectrum(
		Scan scan,
		DataEncoding dataEnc,
		PrecursorMzComputation precComp,
		float mzTolPPM,
		float intensityCutoff,
		boolean exportProlineTitle
	) throws SQLiteException, StreamCorruptedException {

		String mzFragFormat = null;
		// FIXME: check if is_high_res parameter is used and is correct
		if (dataEnc.getPeakEncoding() == PeakEncoding.LOW_RES_PEAK) {
			mzFragFormat = "%.1f";
		} else { // We assume high resolution m/z for fragments
			mzFragFormat = "%.3f";
		}

		// Unpack data
		final ScanHeader scanHeader = scan.getHeader();
		String title;
		if( exportProlineTitle == false ) title = this.titleByScanId.get(scanHeader.getScanId());
		else {
			title = String.format("first_cycle:%d;last_cycle:%d;first_scan:%d;last_scan:%d;first_time:%.02f;last_time:%.02f;raw_file_name:%s;",
				scanHeader.getCycle(),
				scanHeader.getCycle(),
				scanHeader.getInitialId(),
				scanHeader.getInitialId(),
				scanHeader.getTime(),
				scanHeader.getTime(),
				mzDbReader.getFirstSourceFileName()
			);
		}
		
		final float time = scanHeader.getElutionTime();
		double precMz = scanHeader.getPrecursorMz(); // main precursor m/z

		if (precComp == PrecursorMzComputation.SELECTED_ION_MZ) {
			try {
				Precursor precursor = scanHeader.getPrecursor();
				precMz = precursor.parseFirstSelectedIonMz();
			} catch (Exception e) {
				this.logger.error("Selected ion m/z value not found: fall back to default", e);
			}
		} else if (precComp == PrecursorMzComputation.REFINED) {
			
			try {
				Precursor precursor = scanHeader.getPrecursor();
				precMz = precursor.parseFirstSelectedIonMz();
				precMz = this.refinePrecMz(precursor, precMz, mzTolPPM, time, 5);
			} catch (Exception e) {
				this.logger.error("Refined precursor m/z computation failed: fall back to default", e);
			}
			
			/*if (Math.abs(refinedPrecMz - precMz) > 0.5) {
				System.out.println("" + precMz + ", " + refinedPrecMz + ", " + thermoTrailer);
			}

			if (Math.abs(refinedPrecMz - thermoTrailer) > 0.5) {
				System.out.println("" + precMz + ", " + refinedPrecMz + ", " + thermoTrailer);
			}*/

		} else if (precComp == PrecursorMzComputation.REFINED_THERMO) {
			try {
				// TODO: use ScanHeaderReader.loadScanList instead (better perf)
				scanHeader.loadScanList(this.mzDbReader);
				UserParam precMzParam = scanHeader.getScanList().getScans().get(0)
						.getUserParam("[Thermo Trailer Extra]Monoisotopic M/Z:");

				precMz = Double.parseDouble(precMzParam.getValue());
			} catch (NullPointerException e) {
				this.logger.error("Refined thermo value not found: fall back to default");
			}
		} else if (precComp == PrecursorMzComputation.EXTRACTED) {
			
			try {
				Precursor precursor = scanHeader.getPrecursor();
				precMz = precursor.parseFirstSelectedIonMz();
				precMz = this.extractPrecMz(precursor, precMz, mzTolPPM, scanHeader, 5);
			} catch (Exception e) {
				this.logger.error("Extracted precursor m/z computation failed: fall back to default", e);
			}
			
			/*if (Math.abs(refinedPrecMz - precMz) > 0.5) {
				System.out.println("" + precMz + ", " + refinedPrecMz + ", " + thermoTrailer);
			}

			if (Math.abs(refinedPrecMz - thermoTrailer) > 0.5) {
				System.out.println("" + precMz + ", " + refinedPrecMz + ", " + thermoTrailer);
			}*/

		}/* else if (precComp == PrecursorMzComputation.REFINED_MZDB) {
			try {
				precMz = Double.parseDouble(scanHeader.getUserParam(
						PrecursorMzComputation.REFINED_MZDB.getUserParamName()).getValue());
			} catch (NullPointerException e) {
				this.logger.trace("Refined mdb user param name not found: fall back to default");
			}
		}*/

		final int charge = scanHeader.getPrecursorCharge();
		final MgfHeader mgfScanHeader = charge != 0 ? new MgfHeader(title, precMz, charge, time) : new MgfHeader(title, precMz, time);

		StringBuilder spectrumStringBuilder = new StringBuilder();
		mgfScanHeader.appendToStringBuilder(spectrumStringBuilder);

		// Scan Data
		final ScanData data = scan.getData();
		final double[] mzs = data.getMzList();
		final float[] ints = data.getIntensityList();
		final float[] leftHwhms = data.getLeftHwhmList();
		final float[] rightHwhms = data.getRightHwhmList();

		final int intsLength = ints.length;

		//final double[] intsAsDouble = new double[intsLength];
		//for (int i = 0; i < intsLength; ++i) {
		//	intsAsDouble[i] = (double) ints[i];
		//}
		//final double intensityCutOff = 0.0; // new Percentile().evaluate(intsAsDouble, 5.0);

		for (int i = 0; i < intsLength; ++i) {
			
			float intensity = (float) Math.pow(ints[i], 1.5 ); // ^ 3/2
			/*if( dataEnc.getMode().equals(DataMode.FITTED) ) {
			  float peakIntensity = ints[i];
			  float leftHwhm = leftHwhms[i];
			  float rightHwhm = rightHwhms[i];
			  float fwhm = leftHwhm + rightHwhm;
			  logger.debug("leftHwhm:"+leftHwhm);
			  if( fwhm != 0 ) {
			    logger.trace("fwhm:" +fwhm);
			  }
			  // Approximate the area using a triangle area computation
			  // TODO: use a more sophisticated mathematical function
			  intensity = peakIntensity * fwhm * 1e6f;
			} else {
			  intensity = ints[i];
			}*/

			if (intensity >= intensityCutoff) {
				double mz = mzs[i];

				spectrumStringBuilder
					.append(String.format(mzFragFormat, mz))
					.append(" ")
					.append(String.format("%.0f", intensity))
					.append(LINE_SPERATOR);
			}
		}

		spectrumStringBuilder.append(MgfField.END_IONS);

		return spectrumStringBuilder.toString();
	}

	/**
	 * Refines the provided target m/z value by looking at the nearest value in the survey.
	 * 
	 * @param precMz the precursor m/z value to refine
	 * @return the refined precursor m/z value
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	protected double refinePrecMz(Precursor precursor, double precMz, double mzTolPPM, float time, float timeTol)
			throws StreamCorruptedException, SQLiteException {
		
		// Do a XIC in the isolation window and around the provided time
		final ScanSlice[] scanSlices = this._getScanSlicesInIsolationWindow(precursor, time, timeTol);
		if( scanSlices == null ) {
			return precMz;
		}
		
		final ArrayList<Peak> peaks = new ArrayList<Peak>();
		for (ScanSlice sl : scanSlices) {
			Peak p = sl.getNearestPeak(precMz, mzTolPPM);
			if (p != null) {
				p.setLcContext(sl.getHeader());
				peaks.add(p);
			}
		}
		
		// Take the median value of mz
		if (peaks.isEmpty()) {
			MgfWriter.precNotFound++;
			/*
			 * this.logger.warn(lowerMzWindow +", " + upperMzWindow + ", " + targetMz);
			 * this.logger.warn("No peaks in XIC, that's sucks!");
			 */
			return precMz;
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
	 * Detects isotopic pattern in the survey and return the most probable mono-isotopic m/z value
	 * 
	 * @param centerMz the m/z value at the center of the isolation window
	 * @return
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	// TODO: it should be nice to perform this operation in mzdb-processing
	// This requires that the MgfWriter is be moved to this package
	protected double extractPrecMz(Precursor precursor, double precMz, double mzTolPPM, ScanHeader scanHeader, float timeTol)
			throws StreamCorruptedException, SQLiteException {
		
		long sid = scanHeader.getId();
		float time = scanHeader.getTime();

		// Do a XIC in the isolation window and around the provided time
		// FIXME: isolation window is not available for AbSciex files yet
		// final ScanSlice[] scanSlices = this._getScanSlicesInIsolationWindow(precursor, time, timeTol);
		final ScanSlice[] scanSlices = this.mzDbReader.getScanSlices(precMz - 1, precMz + 1, time - timeTol,
			time + timeTol, 1);

		// TODO: perform the operation on all loaded scan slices ???
		ScanSlice nearestScanSlice = null;
		for (ScanSlice sl : scanSlices) {
			if (nearestScanSlice == null)
				nearestScanSlice = sl;
			else if (Math.abs(sl.getHeader().getElutionTime() - time) < Math.abs(nearestScanSlice.getHeader()
					.getElutionTime() - time))
				nearestScanSlice = sl;
		}

		Peak curPeak = nearestScanSlice.getNearestPeak(precMz, mzTolPPM);
		if (curPeak == null)
			return precMz;

		final ArrayList<Peak> previousPeaks = new ArrayList<Peak>();

		for (int putativeZ = 2; putativeZ <= 4; putativeZ++) {

			// avgIsoMassDiff = 1.0027
			double prevPeakMz = precMz + (1.0027 * -1 / putativeZ);
			Peak prevPeak = nearestScanSlice.getNearestPeak(prevPeakMz, mzTolPPM);

			if (prevPeak != null) {
				prevPeak.setLcContext(nearestScanSlice.getHeader());

				double prevPeakExpMz = prevPeak.getMz();
				double approxZ = 1 / Math.abs(precMz - prevPeakExpMz);
				double approxMass = precMz * approxZ - approxZ * MsUtils.protonMass;

				if (approxMass > 2000 && approxMass < 7000) {

					// TODO: find a solution for high mass values
					float minIntRatio = (float) (1400.0 / approxMass); // inferred from lookup table
					float maxIntRatio = Math.min((float) (2800.0 / approxMass), 1); // inferred from lookup table

					// Mass Min Max
					// 2000 0.7 1.4
					// 2500 0.56 1.12
					// 3000 0.47 0.93
					// 3500 0.4 0.8
					// 4000 0.35 0.7
					// 4500 0.31 0.62
					// 5000 0.28 0.56
					// 6000 0.23 0.47
					// 7000 0.2 0.4

					// Check if intensity ratio is valid (in the expected theoretical range)
					// TODO: analyze the full isotope pattern
					float intRatio = prevPeak.getIntensity() / curPeak.getIntensity();

					if (intRatio > minIntRatio && intRatio < maxIntRatio) {

						// Check if there is no next peak with a different charge state that could explain
						// this previous peak
						boolean foundInterferencePeak = false;
						double interferencePeakMz = 0.0;
						for (int interferenceZ = 1; interferenceZ <= 6; interferenceZ++) {
							if (interferenceZ != putativeZ) {
								interferencePeakMz = prevPeakExpMz + (1.0027 * +1 / interferenceZ);
								Peak interferencePeak = nearestScanSlice.getNearestPeak(interferencePeakMz, mzTolPPM);

								// If there is no defined peak with higher intensity
								if (interferencePeak != null && interferencePeak.getIntensity() > prevPeak.getIntensity()) {
									foundInterferencePeak = true;
									break;
								}
							}
						}

						if (foundInterferencePeak == false) {
							logger.debug("Found better m/z value for precMz=" + precMz + " at scan id=" + sid
									+ " with int ratio=" + intRatio + " and z=" + putativeZ + " : "+ prevPeakExpMz);
							previousPeaks.add(prevPeak);
						} else {
							logger.debug("Found interference m/z value for precMz=" + precMz + " at scan id="+ sid + " : " + interferencePeakMz);
						}
					}
				}
			}
		}

		int nbPrevPeaks = previousPeaks.size();
		if (nbPrevPeaks == 0)
			return precMz;

		Collections.sort(previousPeaks, Peak.getIntensityComp());
		Peak mostIntensePrevPeak = previousPeaks.get( previousPeaks.size() - 1 );
		
		return mostIntensePrevPeak.getMz();
	}
	
	private ScanSlice[] _getScanSlicesInIsolationWindow(Precursor precursor, float time, float timeTol)
		throws StreamCorruptedException, SQLiteException {
		
		// do a XIC over isolation window
		final IsolationWindowParamTree iw = precursor.getIsolationWindow();
		if( iw == null ) {
			return null;
		}
		
		CVEntry[] cvEntries = new CVEntry[] {
			CVEntry.ISOLATION_WINDOW_LOWER_OFFSET,
			CVEntry.ISOLATION_WINDOW_TARGET_MZ,
			CVEntry.ISOLATION_WINDOW_UPPER_OFFSET
		};
		final CVParam[] cvParams = iw.getCVParams( cvEntries );
		
		final float lowerMzOffset = Float.parseFloat(cvParams[0].getValue());
		final float targetMz = Float.parseFloat(cvParams[1].getValue());
		final float upperMzOffset = Float.parseFloat(cvParams[2].getValue());
		final double minmz = targetMz - lowerMzOffset;
		final double maxmz = targetMz + upperMzOffset;
		final double minrt = time - timeTol;
		final double maxrt = time + timeTol;
		
		return this.mzDbReader.getScanSlices(minmz, maxmz, minrt, maxrt, 1);	
	}



}
