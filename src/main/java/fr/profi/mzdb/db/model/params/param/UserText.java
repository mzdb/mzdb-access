/**
 * 
 */
package fr.profi.mzdb.db.model.params.param;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

/**
 * @author Marco
 */

//@XmlAccessorType(XmlAccessType.FIELD)
public class UserText {
  @XmlAttribute
  protected String name;
  @XmlValue
  protected String text;
  @XmlAttribute
  protected String type;
  @XmlAttribute
  protected String cvRef = "MS";
  @XmlAttribute
  protected String accession;

  public String getName() {
    return name;
  }

  public String getText() {
    return text;
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
