package fr.profi.mzdb.io.reader;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.sax.SAXSource;

import org.xml.sax.SAXException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.ComponentList;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.Precursor;
import fr.profi.mzdb.db.model.params.ScanList;
import fr.profi.mzdb.utils.jaxb.XercesSAXParser;

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
			SAXSource source = XercesSAXParser.getSAXSource( paramTreeAsStr );
			paramTree = (ParamTree) MzDbReader.paramTreeUnmarshaller.unmarshal(source);
		} catch (JAXBException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		
		return paramTree;
	}

	synchronized public static ScanList parseScanList(String scanListAsStr) {

		ScanList scanList = null;
		
		try {
			SAXSource source = XercesSAXParser.getSAXSource( scanListAsStr );
			scanList = (ScanList) MzDbReader.scanListUnmarshaller.unmarshal(source);
		} catch (JAXBException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		
		return scanList;
	}

	synchronized public static Precursor parsePrecursor(String precursorAsStr) {
		Precursor prec = null;
		
		try {
			SAXSource source = XercesSAXParser.getSAXSource( precursorAsStr );
			prec = (Precursor) MzDbReader.precursorUnmarshaller.unmarshal(source);
		} catch (JAXBException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
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
			SAXSource source = XercesSAXParser.getSAXSource( paramTreeAsStr );
			paramTree = (ComponentList) MzDbReader.instrumentConfigUnmarshaller.unmarshal(source);
		} catch (JAXBException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		
		ParamTreeParser.class.notify();
		
		return paramTree;
	}

}
