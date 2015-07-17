package fr.profi.mzdb.io.writer.mgf;

/** */
public enum PrecursorMzComputation {
	EXTRACTED("extracted mz value from survey"),
	MAIN_PRECURSOR_MZ("main precursor mz"),
	SELECTED_ION_MZ("selected ion mz"),
	REFINED("mzdb-access refined precursor mz"),
	REFINED_THERMO("Thermo refined precursor mz");

	private final String paramName;

	PrecursorMzComputation(String f) {
		this.paramName = f;
	}

	public String getUserParamName() {
		return this.paramName;
	}

}