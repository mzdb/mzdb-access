package fr.profi.mzdb.io.reader.iterator;

import java.io.StreamCorruptedException;
import java.util.Iterator;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.Scan;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.sqlite.ISQLiteStatementConsumer;

//import static fr.profi.mzdb.utils.lambda.JavaStreamExceptionWrappers.rethrowConsumer;

public class MsScanIterator extends AbstractScanSliceIterator implements Iterator<Scan> {

	private static String sqlQuery = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level= ?";
	protected int scanSliceIdx;

	protected ScanSlice[] scanSliceBuffer = null;
	protected boolean bbHasNext = true;

	public MsScanIterator(MzDbReader inst, final int msLevel) throws SQLiteException, StreamCorruptedException {
		//super(inst, sqlQuery, msLevel, rethrowConsumer( (stmt) -> stmt.bind(1, msLevel) ) ); // Bind msLevel
		super(inst, sqlQuery, msLevel, new ISQLiteStatementConsumer() {
			public void accept(SQLiteStatement stmt) throws SQLiteException {
				stmt.bind(1, msLevel); // Bind msLevel
			}
		} );

		this.initScanSliceBuffer();
	}

	protected void initScanSliceBuffer() {

		// init stuff
		// if (! bbHasNext) {
		// scanSliceBuffer = null;
		// return;
		// }

		this.scanSliceBuffer = this.firstBB.toScanSlices();

		// for( ScanSlice sSlice: scanSliceBuffer) {
		// ScanHeader header = mzdb.getScanHeader(sSlice.scanId);
		// sSlice.resizeDataArrays(header.peaksCount);
		// }

		this.scanSliceIdx = 0;

		// Build scan slice buffer
		while (bbHasNext = boundingBoxIterator.hasNext()) {// bbHasNext=

			BoundingBox bb = boundingBoxIterator.next();
			ScanSlice[] sSlices = (ScanSlice[]) bb.toScanSlices();

			if (sSlices == null)
				continue;

			if (sSlices[0].getScanId() == scanSliceBuffer[0].getScanId()) {
				for (int i = 0; i < sSlices.length; i++) {
					scanSliceBuffer[i].getData().addScanData(sSlices[i].getData());// ,
					// scanSliceBuffer[i].length);
				}
			} else {
				// Keep this bounding box for next iteration
				this.firstBB = bb;
				break;
			}
		}
	}

	public Scan next() {

		// firstScanSlices is not null
		int c = scanSliceIdx;
		scanSliceIdx++;

		ScanSlice sSlice = scanSliceBuffer[c];

		// If no more scan slices

		if (scanSliceIdx == scanSliceBuffer.length) {
			if (bbHasNext)
				initScanSliceBuffer();
			else
				this.firstBB = null;
		}

		return sSlice;

	}

}