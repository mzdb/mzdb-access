package fr.profi.mzdb.io.reader.iterator;

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

public class RunSliceIterator extends AbstractScanSliceIterator implements Iterator<RunSlice> {

	// private static String sqlQuery =
	// "SELECT * FROM scan ORDER BY run_slice_id";
	// private static string sqlQuery =
	// "SELECT bounding_box.* FROM bounding_box, run_slice WHERE run_slice.ms_level = ? AND bounding_box.id IN (SELECT id from bounding_box_rtree WHERE ORDER BY run_slice.begin_mz"
	private static String sqlQuery = "SELECT bounding_box.* FROM bounding_box, run_slice WHERE run_slice.ms_level = ? AND bounding_box.run_slice_id = run_slice.id  ORDER BY run_slice.begin_mz";
	protected ScanSlice[] scanSliceBuffer = null;
	protected boolean bbHasNext = true;
	protected final HashMap<Integer, RunSliceHeader> runSliceHeaderById;

	public RunSliceIterator(MzDbReader inst, int msLevel) throws SQLiteException {
		super(inst, sqlQuery, msLevel);
		this.runSliceHeaderById = this.mzDbReader.getRunSliceHeaderById(this.msLevel);
	}

	protected void initScanSliceBuffer() {

		// ArrayList<ScanSlice> merged = new ArrayList<ScanSlice>();
		// ScanSlice[] merged = null;

		this.scanSliceBuffer = this.firstBB.asScanSlicesArray();
		ArrayList<ScanSlice> sl = new ArrayList<ScanSlice>(Arrays.asList(this.scanSliceBuffer));

		while (bbHasNext = boundingBoxIterator.hasNext()) {
			BoundingBox bb = boundingBoxIterator.next();

			if (bb.getRunSliceId() == this.firstBB.getRunSliceId()) {
				sl.addAll(Arrays.asList(bb.asScanSlicesArray()));
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
