package fr.profi.mzdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.*;

import com.almworks.sqlite4java.SQLiteException;

import org.junit.Assert;
import org.junit.Test;

import fr.profi.mzdb.io.reader.cache.MzDbEntityCache;
import fr.profi.mzdb.model.SpectrumSlice;
import fr.profi.mzdb.util.concurrent.Callback;
import fr.profi.mzdb.util.sqlite.SQLite4JavaTest;

public class MultiMzDbReaderTest implements Callback<SpectrumSlice[]> {

    private int callbackCount = 0;
    private static final URL filename_OVEMB150205_12 = MultiMzDbReaderTest.class
	    .getResource("/OVEMB150205_12.raw.0.9.7.mzDB");

	static {
		SQLite4JavaTest.checkSQLite();
	}

    /**
     * Non regression test date: jul 17th 2015
     *
     * @throws URISyntaxException
     * @throws SQLiteException
     * @throws FileNotFoundException
     * @throws ClassNotFoundException
     */
    @Test
    public void readerAsyncTest_OVEMB150205_12()
	    throws URISyntaxException, ClassNotFoundException, FileNotFoundException, SQLiteException {

	final MzDbEntityCache cache = new MzDbEntityCache();
	final double minMz = 00;
	final double maxMz = 1000;
	final float minRt = 100;
	final float maxRt = 10000;

	final File file_OVEMB150205_12 = new File(filename_OVEMB150205_12.toURI());
	// try {
	// MzDbReader mzDb = new MzDbReader(MzDbReaderAsyncTest.class.getResource(filename).getFile(), true);
	// SpectrumSlice[] spectrumSlices1 = mzDb.getMsSpectrumSlicesAsync(minMz, maxMz, minRt, maxRt, null).get();
	// System.out.println(spectrumSlices1.length);
	// } catch (InterruptedException | ExecutionException | StreamCorruptedException | SQLiteException
	// | ClassNotFoundException | FileNotFoundException e) {
	// e.printStackTrace();
	// Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + filename);
	// }
	// create Reader
	Assert.assertEquals(0, this.callbackCount);
	ExecutorService executor = Executors.newFixedThreadPool(5);
	try {
	    // create reader in main thread
	    Assert.assertTrue("file does not exist", file_OVEMB150205_12.isFile());
	    MzDbReader mzDb = new MzDbReader(file_OVEMB150205_12, true);
	    Assert.assertNotNull("invalid file", mzDb);

	    // create a thread and launch a request
	    FutureTask<SpectrumSlice[]> futureTask0 = new FutureTask<SpectrumSlice[]>(new Callable<SpectrumSlice[]>() {

		@Override
		public SpectrumSlice[] call() throws Exception {
		    System.out.println("file = " + file_OVEMB150205_12);
		    MzDbReader mzDb = new MzDbReader(file_OVEMB150205_12, true);
		    System.out.println("new reader created");
		    return mzDb.getMsSpectrumSlices(minMz, maxMz, minRt, maxRt);
		}
	    });
	    executor.execute(futureTask0);
	    SpectrumSlice[] spectrumSlices0 = futureTask0.get();
	    Assert.assertEquals("invalid number of slices", 80, spectrumSlices0.length);
	    Assert.assertEquals(0, this.callbackCount);

	    // use helper method to launch a request
	    Future<SpectrumSlice[]> spectrumSlicesFuture1 = Executors.newSingleThreadExecutor()
		    .submit(MzDbReaderHelper.getSpectrumSlicesInRanges(minMz, maxMz, minRt, maxRt,
			    file_OVEMB150205_12, cache, this));

	    SpectrumSlice[] spectrumSlices1 = spectrumSlicesFuture1.get();
	    Assert.assertEquals(1, this.callbackCount);

	    Assert.assertEquals("invalid number of slices", 80, spectrumSlices1.length);

	    // launch two request at the same time
	    FutureTask<SpectrumSlice[]> futureTask2 = new FutureTask<SpectrumSlice[]>(new Callable<SpectrumSlice[]>() {

		@Override
		public SpectrumSlice[] call() throws Exception {
		    MzDbReader mzDb = new MzDbReader(file_OVEMB150205_12, true);
		    System.out.println("new reader created");
		    return mzDb.getMsSpectrumSlices(minMz, maxMz, minRt, maxRt);
		}
	    });
	    FutureTask<SpectrumSlice[]> futureTask3 = new FutureTask<SpectrumSlice[]>(new Callable<SpectrumSlice[]>() {

		@Override
		public SpectrumSlice[] call() throws Exception {
		    MzDbReader mzDb = new MzDbReader(file_OVEMB150205_12, true);
		    System.out.println("new reader created");
		    return mzDb.getMsSpectrumSlices(minMz, maxMz, minRt, maxRt);
		}
	    });
	    Assert.assertEquals(1, this.callbackCount);
	    executor.execute(futureTask2);
	    executor.execute(futureTask3);

	    SpectrumSlice[] spectrumSlices2 = futureTask2.get();
	    SpectrumSlice[] spectrumSlices3 = futureTask3.get();
	    Assert.assertEquals(1, this.callbackCount);
	    Assert.assertEquals("invalid number of slices", 80, spectrumSlices2.length);
	    Assert.assertEquals("invalid number of slices", 80, spectrumSlices3.length);

	    Assert.assertEquals(1, this.callbackCount);
	    Future<SpectrumSlice[]> spectrumSlicesFuture4 = Executors.newSingleThreadExecutor()
		    .submit(MzDbReaderHelper.getSpectrumSlicesInRanges(minMz, maxMz, minRt, maxRt,
			    file_OVEMB150205_12, cache, this));
	    SpectrumSlice[] spectrumSlices4 = spectrumSlicesFuture4.get();
	    Future<SpectrumSlice[]> spectrumSlicesFuture5 = Executors.newSingleThreadExecutor()
		    .submit(MzDbReaderHelper.getSpectrumSlicesInRanges(minMz, maxMz, minRt, maxRt,
			    file_OVEMB150205_12, cache, this));
	    SpectrumSlice[] spectrumSlices5 = spectrumSlicesFuture5.get();

	    Assert.assertEquals("invalid number of slices", 80, spectrumSlices4.length);
	    Assert.assertEquals("invalid number of slices", 80, spectrumSlices5.length);
	    Assert.assertEquals(3, this.callbackCount);

	} catch (InterruptedException | ExecutionException e) {
	    e.printStackTrace();
	    Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for "
		    + file_OVEMB150205_12.getAbsolutePath());
	}

	System.out.print(".");

	System.out.println(" OK");
    }

    /*
     * (non-Javadoc)
     *
     * @see fr.profi.mzdb.utils.future.FutureCallback#onCompletion(java.lang.Object)
     */
    @Override
    public void onCompletion(SpectrumSlice[] result) {
	this.callbackCount++;

    }
}
