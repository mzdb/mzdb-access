package fr.profi.mzdb.db.model;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.io.reader.ParamTreeParser;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;

// TODO: Auto-generated Javadoc
/**
 * The Class SourceFile.
 * 
 * @author David Bouyssie
 */
public class SourceFile extends AbstractTableModel {

	/** The name. */
	protected String name;

	/** The location. */
	protected String location;

	/**
	 * Instantiates a new source file.
	 * 
	 * @param id
	 *            the id
	 * @param name
	 *            the name
	 * @param location
	 *            the location
	 * @param paramTree
	 *            the param tree
	 */
	public SourceFile(int id, String name, String location, ParamTree paramTree) {
		super(id, paramTree);
		this.name = name;
		this.location = location;
	}

	/**
	 * Instantiates a new source file.
	 * 
	 * @param id
	 *            the id
	 * @param name
	 *            the name
	 * @param location
	 *            the location
	 */
	public SourceFile(int id, String name, String location) {
		this(id, name, location, null);
	}

	/**
	 * Gets the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the location.
	 * 
	 * @return the location
	 */
	public String getLocation() {
		return location;
	}

  @Override
  public void loadParamTree(MzDbReader mzDbReader) throws SQLiteException {
    if ( ! this.hasParamTree()) {
      String sqlString = "SELECT param_tree FROM source_file";
      String paramTreeAsStr =  new SQLiteQuery(mzDbReader.getConnection(), sqlString).extractSingleString();
      this.paramTree = ParamTreeParser.parseParamTree(paramTreeAsStr);
    }
  }

}
