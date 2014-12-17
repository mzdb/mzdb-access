package fr.profi.mzdb.db.model.params;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "precursor")
public class Precursor {
  
  @XmlAttribute(required=true)
  protected String spectrumRef;
  
  @XmlElement(name="isolationWindow")
  protected IsolationWindow isolationWindow;
  
  @XmlElement(name="selectedIonList")
  protected SelectedIonList selectedIonList;
  
  @XmlElement(name="activation")
  protected Activation activation;
  
  public String getSpectrumRef() {
      return spectrumRef;
  }
  
  public IsolationWindow getIsolationWindow() {
      return isolationWindow;
  }
  
  public Activation getActivation() {
      return activation;
  }
  
  public SelectedIonList getSelectedIonList() {
      return selectedIonList;
  }
}
 