package fr.profi.mzdb.db.model.params;

import java.util.List;

import javax.xml.bind.annotation.XmlElementWrapper;

public class Scan extends AbstractParamTree {	

	@XmlElementWrapper
	protected List<ScanWindowList> scanWindowList;

	public List<ScanWindowList> getScanWindowList() {
		return scanWindowList;
	}

	public Scan() {
	}
	
}
