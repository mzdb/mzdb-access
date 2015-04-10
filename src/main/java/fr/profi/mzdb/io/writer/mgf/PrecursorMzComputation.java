package fr.profi.mzdb.io.writer.mgf;

/** */
public enum PrecursorMzComputation {
	DEFAULT("default precursor mz"), REFINED_PWIZ("pwiz refined precursor mz"), REFINED_MZDB("mzdb refined precursor mz");

	private final String paramName;

	PrecursorMzComputation(String f) {
		this.paramName = f;
	}

	public String getUserParamName() {
		return this.paramName;
	}

}