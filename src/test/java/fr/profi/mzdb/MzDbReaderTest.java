package fr.profi.mzdb;

import java.io.FileNotFoundException;
import java.io.StreamCorruptedException;
import java.util.Iterator;
import java.util.List;

import com.almworks.sqlite4java.SQLiteException;

import org.junit.Assert;
import org.junit.Test;

import fr.profi.mzdb.db.model.Run;
import fr.profi.mzdb.db.model.Sample;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.db.model.params.param.UserText;
import fr.profi.mzdb.io.reader.iterator.SpectrumIterator;
import fr.profi.mzdb.model.*;

public class MzDbReaderTest {

    private static final float FLOAT_EPSILON = 1E-4f;
    private static final BBSizes expectedBBSizes_OVEMB150205_12 = new BBSizes(5, 10000, 15, 15);
    private static final int expectedBBCount_OVEMB150205_12 = 3406;
    private static final int expectedCycleCount_OVEMB150205_12 = 158;
    private static final int expectedRunSliceCount_OVEMB150205_12 = 161;
    private static final int expectedSpectrumCount_OVEMB150205_12 = 1193;
    private static final int expectedDataEncodingCount_OVEMB150205_12 = 3;
    private static final int expectedMaxMSLevel_OVEMB150205_12 = 2;
    private static final int expectedCvParamsCount_OVEMB150205_12__0_9_7 = 0;
    private static final int expectedCvParamsCount_OVEMB150205_12__0_9_8 = 1;
    private static final float expectedLastRTTime_OVEMB150205_12 = 240.8635f;
    private static final String expectedModelVersion_OVEMB150205_12_0_9_7 = "0.6";
    private static final String expectedModelVersion_OVEMB150205_12_0_9_8 = "0.7";
    private static final AcquisitionMode expectedAcquisitionMode_OVEMB150205_12__0_9_7 = AcquisitionMode.UNKNOWN;
    private static final AcquisitionMode expectedAcquisitionMode_OVEMB150205_12__0_9_8 = AcquisitionMode.DDA;
    private static final IsolationWindow[] expectedDiaIsolationWindows_OVEMB150205_12 = {};

    private static final float minMz_OVEMB150205_12 = 400f;
    private static final float maxMz_OVEMB150205_12 = 600f;
    private static final float minRt_OVEMB150205_12 = 100f;
    private static final float maxRt_OVEMB150205_12 = 200f;
    private static final int expectedSpectrumSlicesCount_OVEMB150205_12 = 63;
    private static final double expectedSumIntensities_OVEMB150205_12__0_9_7 = 2.543672190435547E9;
    private static final double expectedSumIntensities_OVEMB150205_12__0_9_8 = 2.5717392830078125E9;
    private static final double expectedSumMz_OVEMB150205_12__0_9_7 = 3.868285366432487E7;
    private static final double expectedSumMz_OVEMB150205_12__0_9_8 = 3.867483975354004E7;
    private static final int expectedNbIntensities_OVEMB150205_12__0_9_7 = 155874;
    private static final int expectedNbIntensities_OVEMB150205_12__0_9_8 = 155838;
    private static final int expectedNbPeaks_OVEMB150205_12__0_9_7 = 0;
    private static final int expectedNbPeaks_OVEMB150205_12__0_9_8 = 0;

    String filename__0_9_7 = "/OVEMB150205_12.raw.0.9.7.mzDB";
    String filename__0_9_8 = "/OVEMB150205_12.raw.0.9.8.mzDB";

    /**
     * Non regression test date: jul 27th 2015
     */
    @Test
    public void readerTest_OVEMB150205_12__0_9_7() {
	this.readerTest(this.filename__0_9_7, expectedModelVersion_OVEMB150205_12_0_9_7,
		expectedSumIntensities_OVEMB150205_12__0_9_7, expectedSumMz_OVEMB150205_12__0_9_7,
		expectedNbIntensities_OVEMB150205_12__0_9_7, expectedNbPeaks_OVEMB150205_12__0_9_7,
		expectedCvParamsCount_OVEMB150205_12__0_9_7, expectedAcquisitionMode_OVEMB150205_12__0_9_7);
	System.out.println(" OK");
    }

    /**
     * Non regression test date: jul 27th 2015
     */
    @Test
    public void readerTest_OVEMB150205_12__0_9_8() {
	this.readerTest(this.filename__0_9_8, expectedModelVersion_OVEMB150205_12_0_9_8,
		expectedSumIntensities_OVEMB150205_12__0_9_8, expectedSumMz_OVEMB150205_12__0_9_8,
		expectedNbIntensities_OVEMB150205_12__0_9_8, expectedNbPeaks_OVEMB150205_12__0_9_8,
		expectedCvParamsCount_OVEMB150205_12__0_9_8, expectedAcquisitionMode_OVEMB150205_12__0_9_8);
	System.out.println(" OK");
    }

    public void readerTest(String filename, String expectedModelVersion, double expectedSumIntensities,
	    double expectedSumMz, int expectedNbIntensities, int expectedNbPeaks, int expectedCvParamsCount, AcquisitionMode expectedAcquisitionMode) {
	MzDbReader mzDb = null;

	System.out.print("Non Regression test reading mzDB file " + filename + ": ");
	// create Reader
	try {
	    mzDb = new MzDbReader(MzDbReaderTest.class.getResource(filename).getFile(), true);

	} catch (ClassNotFoundException | FileNotFoundException | SQLiteException e) {
	    Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + filename);
	}
	Assert.assertNotNull("Reader cannot be created", mzDb);
	System.out.print(".");

	// Bounding boxes size
	try {
	    BBSizes bbSizes = mzDb.getBBSizes();
	    Assert.assertEquals("BBSize " + filename + " invalid", expectedBBSizes_OVEMB150205_12, bbSizes);
	} catch (SQLiteException e) {
	    Assert.fail("BBSizes exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// Bounding boxes count
	try {
	    int bbCount = mzDb.getBoundingBoxesCount();
	    Assert.assertEquals("BBCount " + filename + " invalid", expectedBBCount_OVEMB150205_12, bbCount);
	} catch (SQLiteException e) {
	    Assert.fail("BBCount exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// Cycle count
	try {
	    int cycleCount = mzDb.getCyclesCount();
	    Assert.assertEquals("CycleCount " + filename + " invalid", expectedCycleCount_OVEMB150205_12,
		    cycleCount);
	} catch (SQLiteException e) {
	    Assert.fail("CycleCount exception " + e.getMessage() + " for " + filename);
	}

	// Cycle count
	try {
	    int runSliceCount = mzDb.getRunSlicesCount();
	    Assert.assertEquals("RunSliceCount " + filename + " invalid",
		    expectedRunSliceCount_OVEMB150205_12, runSliceCount);
	} catch (SQLiteException e) {
	    Assert.fail("RunSliceCount exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// Spectrum count
	try {
	    int spectrumCount = mzDb.getSpectraCount();
	    Assert.assertEquals("SpectrumCount " + filename + " invalid", expectedSpectrumCount_OVEMB150205_12,
		    spectrumCount);
	} catch (SQLiteException e) {
	    Assert.fail("SpectrumCount exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// Data Encoding count
	try {
	    int dataEncodingCount = mzDb.getDataEncodingsCount();
	    Assert.assertEquals("DataEncodingCount " + filename + " invalid",
		    expectedDataEncodingCount_OVEMB150205_12, dataEncodingCount);
	} catch (SQLiteException e) {
	    Assert.fail("DataEncodingCount exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// Max MS Level
	try {
	    int maxMSLevel = mzDb.getMaxMsLevel();
	    Assert.assertEquals("MaxMSLevel " + filename + " invalid", expectedMaxMSLevel_OVEMB150205_12,
		    maxMSLevel);
	} catch (SQLiteException e) {
	    Assert.fail("MaxMSLevel exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// Max MS Level
	try {
	    float lastRTTime = mzDb.getLastTime();
	    Assert.assertEquals("lastRTTime " + filename + " invalid", expectedLastRTTime_OVEMB150205_12,
		    lastRTTime, FLOAT_EPSILON);
	} catch (SQLiteException e) {
	    Assert.fail("lastRTTime exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// read Model Version
	try {
	    String modelVersion = mzDb.getModelVersion();
	    Assert.assertEquals("ModelVersion " + filename + " invalid", expectedModelVersion, modelVersion);
	} catch (SQLiteException e) {
	    Assert.fail("version exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// read Acquisition Mode
	try {
	    AcquisitionMode acquisitionMode = mzDb.getAcquisitionMode();
	    Assert.assertEquals("AcquisitionMode " + filename + " invalid",
	    		expectedAcquisitionMode, acquisitionMode);
	} catch (SQLiteException e) {
	    Assert.fail("version exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

	// read DIA Isolation Window
	// FIXME: test has
	// try {
	// IsolationWindow[] diaIsolationWindows = mzDb.getDIAIsolationWindows();
	// // System.out.println(diaIsolationWindows.length);
	// // for (IsolationWindow w : diaIsolationWindows) {
	// // System.out.println("-------------------------------------------");
	// // System.out.println(w.getMinMz());
	// // System.out.println(w.getMaxMz());
	// // }
	// Assert.assertArrayEquals("AcquisitionMode " + filename + " invalid", new IsolationWindow[] {},
	// diaIsolationWindows);
	// } catch (SQLiteException e) {
	// Assert.fail("version exception " + e.getMessage() + " for " + filename);
	// }
	System.out.print(".");

	try {
	    SpectrumSlice[] spectrumSlices = mzDb.getMsSpectrumSlices(minMz_OVEMB150205_12, maxMz_OVEMB150205_12,
		    minRt_OVEMB150205_12, maxRt_OVEMB150205_12);
	    Assert.assertNotNull(spectrumSlices);
	    Assert.assertEquals(expectedSpectrumSlicesCount_OVEMB150205_12, spectrumSlices.length);
	    int nbIntensities = 0;
	    int nbPeaks = 0;
	    double sumIntensities = 0;
	    double sumMz = 0;
	    for (SpectrumSlice spectrumSlice : spectrumSlices) {
		for (double intensity : spectrumSlice.getData().getIntensityList()) {
		    sumIntensities += intensity;
		}
		for (double mz : spectrumSlice.getData().getMzList()) {
		    sumMz += mz;
		}
		nbIntensities += spectrumSlice.getData().getIntensityList().length;
		nbIntensities += spectrumSlice.getData().getPeaksCount();
	    }
	    Assert.assertEquals(expectedSumIntensities, sumIntensities, 1);
	    Assert.assertEquals(expectedSumMz, sumMz, 1E-2);
	    Assert.assertEquals(expectedNbIntensities, nbIntensities);
	    Assert.assertEquals(expectedNbPeaks, nbPeaks);
	} catch (StreamCorruptedException | SQLiteException e1) {
	    Assert.fail("spectrum slices extraction throws exception " + e1.getMessage());
	}
	// read Isolation Window
	try {
	    List<Run> runs = mzDb.getRuns();
	    Assert.assertEquals(1, runs.size());
	    List<Sample> samples = mzDb.getSamples();
	    Assert.assertEquals(1, samples.size());
	    // System.out.println(diaIsolationWindows.length);
	    for (Run run : runs) {
		Assert.assertEquals("OVEMB150205_12", run.getName());
		Assert.assertEquals(1, run.getId());
		List<CVParam> cvParams = run.getCVParams();
		Assert.assertEquals(expectedCvParamsCount, cvParams.size());
		List<UserParam> userParams = run.getUserParams();
		Assert.assertEquals(0, userParams.size());
		List<UserText> userText = run.getUserTexts();
		Assert.assertEquals(0, userText.size());
	    }
	    Assert.assertEquals("UPS1 5fmol R1", samples.get(0).getName());

	    try {
		Iterator<Spectrum> iterator = new SpectrumIterator(mzDb, mzDb.getConnection(), 1);
		int spectrumIndex = 0;
		while (iterator.hasNext()) {
		    Spectrum spectrum = iterator.next();
		    SpectrumData data = spectrum.getData();
		    int s = data.getIntensityList().length;
		    Assert.assertEquals(s, data.getIntensityList().length);
		    Assert.assertEquals(s, data.getMzList().length);
		    Assert.assertEquals(s, data.getLeftHwhmList().length);
		    Assert.assertEquals(s, data.getRightHwhmList().length);
		    spectrumIndex++;
		}
		Assert.assertEquals(expectedCycleCount_OVEMB150205_12, spectrumIndex);
	    } catch (StreamCorruptedException e) {
		e.printStackTrace();
	    }

	} catch (SQLiteException e) {
	    Assert.fail("version exception " + e.getMessage() + " for " + filename);
	}
	System.out.print(".");

    }
}
