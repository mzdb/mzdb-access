/**
 * 
 */
package fr.profi.mzdb.db.model.params;

import java.util.List;

import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.db.model.params.param.UserText;

// TODO: Auto-generated Javadoc
/**
 * The Interface IParamContainer.
 * 
 * @author David Bouyssie
 */
public interface IParamContainer {

	/**
	 * Gets the cV params.
	 * 
	 * @return the cV params
	 */
	public List<CVParam> getCVParams();

	/**
	 * Gets the user params.
	 * 
	 * @return the user params
	 */
	public List<UserParam> getUserParams();
	
	public List<UserText> getUserTexts();
	/**
	 * Gets the user param.
	 * 
	 * @param name
	 *            the name
	 * @return the user param
	 */
	public UserParam getUserParam(String name);

}
