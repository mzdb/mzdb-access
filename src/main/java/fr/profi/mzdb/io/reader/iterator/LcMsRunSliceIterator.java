package fr.profi.mzdb.io.reader.iterator;

import java.io.StreamCorruptedException;
import java.util.Iterator;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.RunSlice;
import fr.profi.mzdb.utils.sqlite.ISQLiteStatementConsumer;

//import static fr.profi.mzdb.utils.lambda.JavaStreamExceptionWrappers.rethrowConsumer;

public class LcMsRunSliceIterator extends AbstractRunSliceIterator implements Iterator<RunSlice> {

	private static String allRunSlicesSqlQuery = "SELECT bounding_box.* FROM bounding_box, run_slice "
			+ "WHERE run_slice.ms_level = ? AND bounding_box.run_slice_id = run_slice.id  ORDER BY run_slice.begin_mz";

	private static String runSlicesSubsetSqlQuery = "SELECT bounding_box.* FROM bounding_box, run_slice "
			+ "WHERE run_slice.ms_level = ? "
			+ "AND bounding_box.run_slice_id = run_slice.id  "
			+ "AND run_slice.end_mz >= ? "
			+ "AND run_slice.begin_mz <= ?"
			+ "ORDER BY run_slice.begin_mz";
	
	private static ISQLiteStatementConsumer createAllRunSlicesStatementBinder() {		
		// Lambda require to catch Exceptions
		// For workarounds see: http://stackoverflow.com/questions/14039995/java-8-mandatory-checked-exceptions-handling-in-lambda-expressions-why-mandato
		//return rethrowConsumer( (stmt) -> stmt.bind(1, 1) ); // Bind the msLevel
		return new ISQLiteStatementConsumer() {
			public void accept(SQLiteStatement stmt) throws SQLiteException {
				stmt.bind(1, 1); // Bind the msLevel
			}
		};
	}
	
	private static ISQLiteStatementConsumer createRunSlicesSubsetStatementBinder(
		final double minRunSliceMz,
		final double maxRunSliceMz
	) {
		// Lambda require to catch Exceptions
		// For workarounds see: http://stackoverflow.com/questions/14039995/java-8-mandatory-checked-exceptions-handling-in-lambda-expressions-why-mandato	
		/*return rethrowConsumer( (stmt) -> {
			stmt.bind(1, 1); // Bind the msLevel
			stmt.bind(2, minRunSliceMz); // Bind the minRunSliceMz
			stmt.bind(3, maxRunSliceMz); // Bind the maxRunSliceMz
		});*/
		
		return new ISQLiteStatementConsumer() {
			public void accept(SQLiteStatement stmt) throws SQLiteException {
				stmt.bind(1, 1); // Bind the msLevel
				stmt.bind(2, minRunSliceMz); // Bind the minRunSliceMz
				stmt.bind(3, maxRunSliceMz); // Bind the maxRunSliceMz
			}
		};
	}

	public LcMsRunSliceIterator(MzDbReader mzDbReader) throws SQLiteException, StreamCorruptedException {
		// Set msLevel to 1
		super(mzDbReader, allRunSlicesSqlQuery, 1, createAllRunSlicesStatementBinder() );
	}

	public LcMsRunSliceIterator(MzDbReader mzDbReader, double minRunSliceMz, double maxRunSliceMz)
			throws SQLiteException, StreamCorruptedException {		
		// Set msLevel to 1
		super(mzDbReader, runSlicesSubsetSqlQuery, 1, createRunSlicesSubsetStatementBinder(minRunSliceMz,maxRunSliceMz) );
	}

}
