package fr.profi.mzdb;

import java.io.FileNotFoundException;

import com.almworks.sqlite4java.SQLiteException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.profi.mzdb.db.model.params.thermo.ThermoFragmentationTarget;
import fr.profi.mzdb.db.model.params.thermo.ThermoScanMetaData;
import fr.profi.mzdb.model.Spectrum;
import fr.profi.mzdb.model.SpectrumHeader;

public class MzDbMetaDataTest {
	
	final Logger logger = LoggerFactory.getLogger(MzDbMetaDataTest.class);

	private static final float FLOAT_EPSILON = 1E-4f;
	private static final double DOUBLE_EPSILON = 1E-14;

	String filename__0_9_7 = "OVEMB150205_12.raw.0.9.7.mzDB";
	String filename__0_9_8 = "OVEMB150205_12.raw.0.9.8.mzDB";

	/**
	 * Non regression test date: june 30th 2016
	 */
	@Test
	public void readerTest_OVEMB150205_12__0_9_7() {
		this.metaDataTest(this.filename__0_9_7);
	}

	/**
	 * Non regression test date: jul 27th 2015
	 */
	@Test
	public void readerTest_OVEMB150205_12__0_9_8() {
		this.metaDataTest(this.filename__0_9_8);
	}

	public void metaDataTest(String filename) {
		MzDbReader mzDb = null;

		System.out.println("Non Regression test for meta-data, reading mzDB file " + filename + ": ");
		
		// Create the Reader
		try {
			mzDb = new MzDbReader(MzDbMetaDataTest.class.getResource("/"+filename).getFile(), true);

		} catch (ClassNotFoundException | FileNotFoundException | SQLiteException e) {
			Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + filename);
		}
		Assert.assertNotNull("Reader cannot be created", mzDb);

		// Perform some checks for Thermo MS2 meta-data
		try {
			mzDb.enableScanListLoading();
			
			Spectrum ms2Spectrum = mzDb.getSpectrum(17);
			SpectrumHeader header = ms2Spectrum.getHeader();
			
			ThermoScanMetaData ms2Meta = header.getScanList().getScans().get(0).getThermoMetaData();
			Assert.assertEquals("MS level check for MS2", 2, ms2Meta.getMsLevel());
			Assert.assertEquals("MS2 acquisition type check", "ITMS", ms2Meta.getAnalyzerType());
			Assert.assertEquals( 30f, ms2Meta.getTargets()[0].getCollisionEnergy(), FLOAT_EPSILON);
			
			// Perform some checks for Thermo MS2&MS3 meta-data
			String ms3FilterString = "FTMS + p NSI sps d Full ms3 707.8472@cid35.00 463.3669@hcd45.00 [115.0000-140.0000]";			
			ThermoScanMetaData ms3Meta = new ThermoScanMetaData(ms3FilterString);
			Assert.assertEquals("MS level check for MS2", 3, ms3Meta.getMsLevel());
			Assert.assertEquals("MS2 acquisition type check", "FTMS", ms3Meta.getAnalyzerType());
			
			ThermoFragmentationTarget ms1Target = ms3Meta.getTargets()[0];
			Assert.assertEquals( 707.8472, ms1Target.getMz(), DOUBLE_EPSILON);
			Assert.assertEquals("Activation type check for MS1 target", "cid", ms1Target.getActivationType());
			Assert.assertEquals( 35f, ms1Target.getCollisionEnergy(), FLOAT_EPSILON);
			
			ThermoFragmentationTarget ms2Target = ms3Meta.getTargets()[1];
			Assert.assertEquals( 463.3669, ms2Target.getMz(), DOUBLE_EPSILON);
			Assert.assertEquals("Activation type check for MS2 target", "hcd", ms2Target.getActivationType());
			Assert.assertEquals( 45f, ms2Target.getCollisionEnergy(), FLOAT_EPSILON);
			
		} catch (Exception e) {
			logger.error("Can't parse ScanList meta-data !", e);
			Assert.fail("Processing file named '" + filename+"' failed because: " + e.getMessage() );
		}
		
		logger.info("Meta-data have been checked for file: "+ filename);

	}
}
