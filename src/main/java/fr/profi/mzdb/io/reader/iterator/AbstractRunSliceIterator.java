package fr.profi.mzdb.io.reader.iterator;

import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.RunSlice;
import fr.profi.mzdb.model.RunSliceData;
import fr.profi.mzdb.model.RunSliceHeader;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.sqlite.ISQLiteStatementConsumer;

public abstract class AbstractRunSliceIterator extends AbstractScanSliceIterator implements Iterator<RunSlice> {

	protected ScanSlice[] scanSliceBuffer = null;
	protected boolean bbHasNext = true;
	protected final HashMap<Integer, RunSliceHeader> runSliceHeaderById;
	
	public AbstractRunSliceIterator(
		MzDbReader mzDbReader,
		String sqlQuery,
		int msLevel,
		ISQLiteStatementConsumer stmtBinder
	) throws SQLiteException, StreamCorruptedException {
		super(mzDbReader, sqlQuery, msLevel, stmtBinder);
		
		this.runSliceHeaderById = this.mzDbReader.getRunSliceHeaderById(msLevel);
	}
	
	protected void initScanSliceBuffer() {
		
		this.scanSliceBuffer = this.firstBB.toScanSlices();
		ArrayList<ScanSlice> sl = new ArrayList<ScanSlice>(Arrays.asList(this.scanSliceBuffer));

		while (bbHasNext = boundingBoxIterator.hasNext()) {
			BoundingBox bb = boundingBoxIterator.next();

			if (bb.getRunSliceId() == this.firstBB.getRunSliceId()) {
				sl.addAll(Arrays.asList(bb.toScanSlices()));
			} else {
				this.firstBB = bb;
				break;
			}
		}

		this.scanSliceBuffer = sl.toArray(new ScanSlice[sl.size()]);

		if (!bbHasNext) {
			this.firstBB = null;
		}
	}

	public RunSlice next() {

		initScanSliceBuffer();

		int runSliceId = this.scanSliceBuffer[0].getRunSliceId();
		RunSliceData rsd = new RunSliceData(runSliceId, this.scanSliceBuffer);
		// rsd.buildPeakListByScanId();

		RunSliceHeader rsh = this.runSliceHeaderById.get(runSliceId);

		return new RunSlice(rsh, rsd);
	}

}
