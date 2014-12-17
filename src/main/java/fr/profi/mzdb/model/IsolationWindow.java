package fr.profi.mzdb.model;

/**
 * 
 * 
 */
public class IsolationWindow {
    
    private final double minMz;
    private final double maxMz;
    
    public IsolationWindow(double minMz, double maxMz) {
	this.minMz = minMz;
	this.maxMz = maxMz;
    }
    
    public double getMinMz() {
	return this.minMz;
    }
    
    public double getMaxMz() {
	return this.maxMz;
    }

}
