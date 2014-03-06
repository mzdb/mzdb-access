package fr.profi.mzdb.db.model.params.param;

import javax.xml.bind.annotation.*;

//@XmlAccessorType(XmlAccessType.FIELD)
public class CVParam {
  
  @XmlAttribute
	protected String value = "";
	
  @XmlAttribute
	protected String cvRef = "MS";
	
  @XmlAttribute
	protected String name = "";
	
  @XmlAttribute
	protected String accession = "";

  @XmlAttribute
  protected String unitCvRef = "UO";
  
  @XmlAttribute
	protected String unitAccession = "";
  
  @XmlAttribute
	protected String unitName = "";

	public String getValue() {
		return value;
	}

	public String getAccession() {
		return accession;
	}

	public String getCvRef() {
		return cvRef;
	}

	public String getName() {
		return name;
	}

	public String getUnitCvRef() {
		return unitCvRef;
	}

	public String getUnitAccession() {
		return unitAccession;
	}

	public String getUnitName() {
		return unitName;
	}

}
