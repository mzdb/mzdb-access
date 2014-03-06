package fr.profi.mzdb.db.model.params;

import java.util.ArrayList;
import java.util.List;

import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.db.model.params.param.UserText;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlElement;
// TODO: Auto-generated Javadoc
/**
 * The Class ParamTree.
 * 
 * @author David Bouyssie
 */
@XmlRootElement(name = "params")
public class ParamTree implements IParamContainer {

	/** The cv params. */
  
	@XmlElement(name="cvParam", type = CVParam.class, required=false)
	@XmlElementWrapper
	protected List<CVParam> cvParams;

	/** The user params. */
	@XmlElement(name="userParam", type = UserParam.class, required=false)
	@XmlElementWrapper
	protected List<UserParam> userParams;
	
	/**The useText params: newly introduced for handling Thermo metadata in
	 * text field
	 */
  @XmlElement(name="userText", type = UserText.class, required= false)
  @XmlElementWrapper
	protected List<UserText> userTexts;

	/**
	 * necessary for jackson.
	 */
	protected ParamTree() {
		super();
	}

	

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.profi.mzdb.db.model.IParamContainer#getCVParams()
	 */
	public List<CVParam> getCVParams() {
		if (this.cvParams == null)
			this.cvParams = new ArrayList<CVParam>();

		return cvParams;
	}

	// most of the object does not contain any UserParam, so this is set to be non
	// abstract
	// to avoid to override it in subclasses
	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.profi.mzdb.db.model.IParamContainer#getUserParams()
	 */
	public List<UserParam> getUserParams() {
		if (this.userParams == null)
			this.userParams = new ArrayList<UserParam>();

		return this.userParams;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.profi.mzdb.db.model.IParamContainer#getUserParam(java.lang.String)
	 */
	public UserParam getUserParam(String name) {
		UserParam p = null;
		for (UserParam up : this.getUserParams()) {
		  //System.out.println(up.getName());
			if (up.getName().equals(name)) {
				p = up;
				break;
			}
		}
		return p;
	}
	
	public List<UserText> getUserTexts() {
    if (this.userTexts == null)
      this.userTexts = new ArrayList<UserText>();
    return this.userTexts;
  }
}
