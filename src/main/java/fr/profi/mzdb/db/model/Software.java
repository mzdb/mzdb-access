package fr.profi.mzdb.db.model;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.io.reader.ParamTreeParser;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;

// TODO: Auto-generated Javadoc
/**
 * The Class Software.
 * 
 * @author David Bouyssie
 */
public class Software extends AbstractTableModel {

	/** The name. */
	protected String name;

	/** The version. */
	protected String version;

	/**
	 * Instantiates a new software.
	 * 
	 * @param id
	 *            the id
	 * @param name
	 *            the name
	 * @param version
	 *            the version
	 * @param paramTree
	 *            the param tree
	 */
	public Software(int id, String name, String version, ParamTree paramTree) {
		super(id, paramTree);
		this.name = name;
		this.version = version;
	}

	/**
	 * Instantiates a new software.
	 * 
	 * @param id
	 *            the id
	 * @param name
	 *            the name
	 * @param version
	 *            the version
	 */
	public Software(int id, String name, String version) {
		this(id, name, version, null);
	}

	/**
	 * Gets the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/*
	 * public void setName(String name) { this.name = name; }
	 */

	/**
	 * Gets the version.
	 * 
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}

  @Override
  public void loadParamTree(MzDbReader mzDbReader) throws SQLiteException {
    if ( ! this.hasParamTree()) {
      String sqlString = "SELECT param_tree FROM software";
      String paramTreeAsStr =  new SQLiteQuery(mzDbReader.getConnection(), sqlString).extractSingleString();
      this.paramTree = ParamTreeParser.parseParamTree(paramTreeAsStr);
    }
  }

	/*
	 * public void setVersion(String version) { this.version = version; }
	 */

}
