package fr.profi.mzdb.io.reader.iterator;

import java.io.StreamCorruptedException;
import java.util.Iterator;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.AbstractMzDbReader;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.Spectrum;
import fr.profi.mzdb.model.SpectrumSlice;
import fr.profi.mzdb.utils.sqlite.ISQLiteStatementConsumer;

//import static fr.profi.mzdb.utils.lambda.JavaStreamExceptionWrappers.rethrowConsumer;

public class SpectrumIterator extends AbstractSpectrumSliceIterator implements Iterator<Spectrum> {

	private static String allMsLevelsSqlQuery = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id";
	private static String singleMsLevelSqlQuery = allMsLevelsSqlQuery + " AND spectrum.ms_level= ?";
	protected int spectrumSliceIdx;

	protected SpectrumSlice[] spectrumSliceBuffer = null;
	protected boolean bbHasNext = true;
	
	public SpectrumIterator(AbstractMzDbReader mzDbReader, SQLiteConnection connection) throws SQLiteException, StreamCorruptedException {
		super(mzDbReader.getSpectrumHeaderReader(), mzDbReader.getDataEncodingReader(), connection, allMsLevelsSqlQuery);

		this.initSpectrumSliceBuffer();
	}

	public SpectrumIterator(AbstractMzDbReader mzDbReader, SQLiteConnection connection, final int msLevel) throws SQLiteException, StreamCorruptedException {
		//super(inst, sqlQuery, msLevel, rethrowConsumer( (stmt) -> stmt.bind(1, msLevel) ) ); // Bind msLevel
		super(
			mzDbReader.getSpectrumHeaderReader(),
			mzDbReader.getDataEncodingReader(),
			connection,
			singleMsLevelSqlQuery,
			msLevel,
			new ISQLiteStatementConsumer() {
				public void accept(SQLiteStatement stmt) throws SQLiteException {
					stmt.bind(1, msLevel); // Bind msLevel
				}
			}
		);

		this.initSpectrumSliceBuffer();
	}

	protected void initSpectrumSliceBuffer() {

		// init stuff
		// if (! bbHasNext) {
		// spectrumSliceBuffer = null;
		// return;
		// }

		this.spectrumSliceBuffer = this.firstBB.toSpectrumSlices();

		// for( SpectrumSlice sSlice: spectrumSliceBuffer) {
		// SpectrumHeader header = mzdb.getSpectrumHeader(sSlice.spectrumId);
		// sSlice.resizeDataArrays(header.peaksCount);
		// }

		this.spectrumSliceIdx = 0;

		// Build spectrum slice buffer
		while (bbHasNext = boundingBoxIterator.hasNext()) {// bbHasNext=

			BoundingBox bb = boundingBoxIterator.next();
			SpectrumSlice[] sSlices = (SpectrumSlice[]) bb.toSpectrumSlices();

			if (sSlices == null)
				continue;

			if (sSlices[0].getSpectrumId() == spectrumSliceBuffer[0].getSpectrumId()) {
				for (int i = 0; i < sSlices.length; i++) {
					spectrumSliceBuffer[i].getData().addSpectrumData(sSlices[i].getData());// ,
					// spectrumSliceBuffer[i].length);
				}
			} else {
				// Keep this bounding box for next iteration
				this.firstBB = bb;
				break;
			}
		}
	}

	public Spectrum next() {

		// firstSpectrumSlices is not null
		int c = spectrumSliceIdx;
		spectrumSliceIdx++;

		SpectrumSlice sSlice = spectrumSliceBuffer[c];

		// If no more spectrum slices

		if (spectrumSliceIdx == spectrumSliceBuffer.length) {
			if (bbHasNext)
				initSpectrumSliceBuffer();
			else
				this.firstBB = null;
		}

		return sSlice;

	}

}