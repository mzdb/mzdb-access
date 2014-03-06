/*
 * Package fr.profi.mzdb.model
 * @author David Bouyssie
 */
package fr.profi.mzdb.model;

// TODO: Auto-generated Javadoc
/**
 * The Enum ByteOrder.
 * 
 * @author David Bouyssie
 */
public enum ByteOrder {

	/** The big endian. */
	BIG_ENDIAN(1234),

	/** The little endian. */
	LITTLE_ENDIAN(4321);

	/** The endianness. */
	private final int endianness;

	/**
	 * Instantiates a new byte order.
	 * 
	 * @param _endianness
	 *            the _endianness
	 */
	private ByteOrder(int _endianness) {
		endianness = _endianness;
	}

	/**
	 * Gets the endianness.
	 * 
	 * @return the endianness
	 */
	public int getEndianness() {
		return endianness;
	}

}