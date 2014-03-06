package fr.profi.mzdb.db.model.params.param;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.FIELD)
public class UserParam {
  @XmlAttribute
  protected String name;

  @XmlAttribute
  protected String value;

  @XmlAttribute
  protected String type;// ="xsd:float"/>;

  @XmlAttribute
  protected String cvRef = "MS";

  @XmlAttribute
  protected String accession = "";

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public String getType() {
    return type;
  }

  public String getCvRef() {
    return cvRef;
  }

  public String getAccession() {
    return accession;
  }

}
