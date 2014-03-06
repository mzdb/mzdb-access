package fr.profi.mzdb.db.model;

import java.util.List;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.IParamContainer;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.db.model.params.param.UserText;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.utils.misc.AbstractInMemoryIdGen;

// TODO: Auto-generated Javadoc
/**
 * The Class AbstractTableModel.
 * 
 * @author David Bouyssie
 */
public abstract class AbstractTableModel extends AbstractInMemoryIdGen implements IParamContainer {

	/** The id. */
	protected int id;

	/** The param tree. */
	protected ParamTree paramTree = null;

	/**
	 * Instantiates a new abstract table model.
	 * 
	 * @param id
	 *            the id
	 * @param paramTree
	 *            the param tree
	 */
	protected AbstractTableModel(int id, ParamTree paramTree) {
		super();
		this.id = id;
		this.paramTree = paramTree;
	}

	/**
	 * Gets the id.
	 * 
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/*
	 * public void setId(int id) { this.id = id; }
	 */

	/**
	 * Checks for param tree.
	 * 
	 * @return true, if successful
	 */
	public boolean hasParamTree() {
		return paramTree != null;
	}

	/**
	 * Gets the param tree.
	 * 
	 * @return the param tree
	 */
	public ParamTree getParamTree(MzDbReader mzDbReader) {
	  if (! this.hasParamTree()){}
	    try {
	      this.loadParamTree(mzDbReader);
	    } catch (SQLiteException e) {
	      System.out.println(e.getMessage());
	    }
		return paramTree;
	}

	/**
	 * Sets the param tree.
	 * 
	 * @param paramTree
	 *            the new param tree
	 */
	public void setParamTree(ParamTree paramTree) {
		this.paramTree = paramTree;
	}
	
  /**
   * Loads the param tree.
   * 
   * @param paramTree
   *            the new param tree
   */
  abstract public void loadParamTree(MzDbReader mzDbReader) throws SQLiteException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.profi.mzdb.db.model.IParamContainer#getCVParams()
	 */
	public List<CVParam> getCVParams() {
		return this.paramTree.getCVParams();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.profi.mzdb.db.model.IParamContainer#getUserParams()
	 */
	public List<UserParam> getUserParams() {
		return this.paramTree.getUserParams();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.profi.mzdb.db.model.IParamContainer#getUserParam(java.lang.String)
	 */
	public UserParam getUserParam(String name) {
		return this.paramTree.getUserParam(name);
	}
	
	public List<UserText> getUserTexts() {
	   return this.paramTree.getUserTexts();
	}

}