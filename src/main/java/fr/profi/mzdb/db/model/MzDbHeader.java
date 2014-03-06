package fr.profi.mzdb.db.model;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.io.reader.ParamTreeParser;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;

/**
 * The Class MzDbHeader.
 * 
 * @author David Bouyssie
 */
public class MzDbHeader extends AbstractTableModel {

	/** The version. */
	protected String version;

	/** The creation timestamp. */
	protected int creationTimestamp;
	
	/**
	 * Instantiates a new mz db header.
	 * 
	 * @param version
	 *            the version
	 * @param creationTimestamp
	 *            the creation timestamp
	 * @param paramTree
	 *            the param tree
	 */
	public MzDbHeader(String version, int creationTimestamp, ParamTree paramTree) {
		super(1, paramTree);
		this.version = version;
		this.creationTimestamp = creationTimestamp;
	}

	/**
	 * Instantiates a new mz db header.
	 * 
	 * @param version
	 *            the version
	 * @param creationTimestamp
	 *            the creation timestamp
	 */
	public MzDbHeader(String version, int creationTimestamp) {
		this(version, creationTimestamp, null);
	}
	
	public String getVersion() {
	  return this.version;
	}

  @Override
  public void loadParamTree(MzDbReader mzDbReader) throws SQLiteException {
    if ( ! this.hasParamTree()) {
      String sqlString = "SELECT param_tree FROM mzdb";
      String paramTreeAsStr =  new SQLiteQuery(mzDbReader.getConnection(), sqlString).extractSingleString();
      this.paramTree = ParamTreeParser.parseParamTree(paramTreeAsStr);
      
    }
    
  }

}
