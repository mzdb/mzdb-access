package fr.profi.mzdb.io.reader.iterator;

import java.io.StreamCorruptedException;
import java.util.Iterator;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.reader.bb.IBlobReader;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.Scan;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.sqlite.ISQLiteStatementConsumer;

//import static fr.profi.mzdb.utils.lambda.JavaStreamExceptionWrappers.rethrowConsumer;

/**
 * @author Marco
 *
 */
public class MsScanRangeIterator implements Iterator<Scan> {

	private int bbStartingScanId;

	private int bbEndingScanId;

	private int wantedStartingScanId;

	private int wantedEndingScanId;

	private MzDbReader mzDbReader;

	private MsScanRangeIteratorImpl _iter;

	private Integer currentId;

	private String sqlQuery;

	private Integer trueLastScanId = 0;

	// private boolean toStop = false;

	public MsScanRangeIterator(MzDbReader inst, int msLevel, int start, int end) throws SQLiteException, StreamCorruptedException {
		this.mzDbReader = inst;
		this.wantedStartingScanId = start;
		this.wantedEndingScanId = end;
		this.bbStartingScanId = mzDbReader.getScanHeader(start).getBBFirstSpectrumId();
		this.bbEndingScanId = mzDbReader.getScanHeader(end).getBBFirstSpectrumId();
		sqlQuery = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level= ? AND "
			+ "bounding_box.first_spectrum_id >= "
			+ this.bbStartingScanId
			+ " AND bounding_box.first_spectrum_id <= " + this.bbEndingScanId;

		this._iter = new MsScanRangeIteratorImpl(mzDbReader, msLevel);
	}

	public class MsScanRangeIteratorImpl extends AbstractScanSliceIterator implements Iterator<Scan> {

		protected int scanSliceIdx;

		protected ScanSlice[] scanSliceBuffer = null;
		protected boolean bbHasNext = true;

		public MsScanRangeIteratorImpl(MzDbReader mzDbReader, final int msLevel) throws SQLiteException,
				StreamCorruptedException {
			//super(mzDbReader, sqlQuery, msLevel, rethrowConsumer( (stmt) -> stmt.bind(1, msLevel) ) ); // Bind msLevel
			super(mzDbReader, sqlQuery, msLevel, new ISQLiteStatementConsumer() {
				public void accept(SQLiteStatement stmt) throws SQLiteException {
					stmt.bind(1, msLevel); // Bind msLevel
				}
			} );
			
			this.initScanSliceBuffer();
		}

		protected void initScanSliceBuffer() {
			this.scanSliceBuffer = this.firstBB.toScanSlices();

			this.scanSliceIdx = 0;

			// Build scan slice buffer
			while (bbHasNext = boundingBoxIterator.hasNext()) {// bbHasNext=

				BoundingBox bb = boundingBoxIterator.next();
				IBlobReader bbReader = bb.getReader();

				if (bb.getLastScanId() > wantedEndingScanId) {
					// FIXME: DBO => it was (bb.nbScans() - 2) when idOfScanAt was based on a 1 value starting index
					// It may cause some issues in the future
					trueLastScanId = bbReader.getScanIdAt(bb.getScansCount() - 1);
				} else if (bb.getLastScanId() == wantedEndingScanId) {
					trueLastScanId = bb.getLastScanId();
				} else {

				}
				ScanSlice[] sSlices = (ScanSlice[]) bb.toScanSlices();

				if (sSlices == null)
					continue;

				if (sSlices[0].getScanId() == scanSliceBuffer[0].getScanId()) {
					for (int i = 0; i < sSlices.length; i++) {
						scanSliceBuffer[i].getData().addScanData(sSlices[i].getData());
					}
				} else {
					// Keep this bounding box for next iteration
					this.firstBB = bb;
					break;
				}
			}
		}

		@Override
		public Scan next() {

			// firstScanSlices is not null
			int c = scanSliceIdx;
			scanSliceIdx++;

			ScanSlice sSlice = scanSliceBuffer[c];

			if (scanSliceIdx == scanSliceBuffer.length) {
				if (bbHasNext)
					initScanSliceBuffer();
				else
					this.firstBB = null;
			}
			if (sSlice.getScanId() < wantedStartingScanId) {
				return null;
			} else if (sSlice.getScanId() > bbEndingScanId && sSlice.getScanId() < wantedEndingScanId) {
				this.firstBB = null;
				currentId = sSlice.getScanId();
				return sSlice;
			} else if (sSlice.getScanId() == wantedEndingScanId) {
				// toStop = true;
				currentId = sSlice.getScanId();
				return sSlice;
			} else if (sSlice.getScanId() > wantedEndingScanId) {
				// toStop = true;
				currentId = null;
				return null;
			} else {
				return sSlice;// do nothing
			}

		}

	};

	@Override
	public boolean hasNext() {
		if (currentId != null && currentId.equals(trueLastScanId))
			return false;
		return true;
	}

	@Override
	public Scan next() {
		Scan sSlice = _iter.next();
		while (_iter.hasNext() && sSlice == null) {// && ! toStop) {
			sSlice = _iter.next();
		}
		currentId = sSlice.getHeader().getId();
		return sSlice;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
	};
}
