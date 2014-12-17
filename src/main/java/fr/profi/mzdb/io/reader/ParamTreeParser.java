package fr.profi.mzdb.io.reader;

import java.io.StringReader;

import javax.xml.bind.JAXBException;

import org.xml.sax.InputSource;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.ComponentList;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.Precursor;
import fr.profi.mzdb.db.model.params.ScanList;

// TODO: Auto-generated Javadoc
/**
 * The Class ParamTreeParser.
 * 
 * @author David Bouyssie
 */
public class ParamTreeParser {

	/**
	 * Parses the param tree.
	 * 
	 * @param paramTreeAsStr The param tree as a String
	 * @return the param tree
	 */
	synchronized public static ParamTree parseParamTree(String paramTreeAsStr) {

		ParamTree paramTree = null;
		
		try {
			paramTree = (ParamTree) MzDbReader.paramTreeUnmarshaller.unmarshal(new StringReader(paramTreeAsStr));
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		
		return paramTree;
	}

	synchronized public static ScanList parseScanList(String scanListAsStr) {

		ScanList scanList = null;
		
		try {
			scanList = (ScanList) MzDbReader.scanListUnmarshaller.unmarshal(new StringReader(scanListAsStr));
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		
		return scanList;
	}

	synchronized public static Precursor parsePrecursor(String precursorAsStr) {
		Precursor prec = null;
		
		try {
			prec = (Precursor) MzDbReader.precursorUnmarshaller.unmarshal(new StringReader(precursorAsStr));
		} catch (JAXBException e) {
			e.printStackTrace();
		}

		return prec;
	}

	/**
	 * Parses the instrument config param tree.
	 * 
	 * @param paramTreeAsStr
	 *            the param tree as str
	 * @return the instrument config param tree
	 */
	synchronized public static ComponentList parseComponentList(String paramTreeAsStr) {

		ComponentList paramTree = null;
		
		try {
			paramTree = (ComponentList) MzDbReader.instrumentConfigUnmarshaller.unmarshal(new InputSource(paramTreeAsStr));
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ParamTreeParser.class.notify();
		
		return paramTree;
	}

}
