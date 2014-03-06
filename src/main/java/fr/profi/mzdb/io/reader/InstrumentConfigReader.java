package fr.profi.mzdb.io.reader;

import java.util.ArrayList;
import java.util.List;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.db.model.InstrumentConfiguration;
import fr.profi.mzdb.db.table.InstrumentConfigurationTable;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;
import fr.profi.mzdb.utils.sqlite.SQLiteRecordIterator;

// TODO: Auto-generated Javadoc
/**
 * The Class InstrumentConfigReader.
 * 
 * @author David Bouyssie
 */
public class InstrumentConfigReader {

	/** The connection. */
	protected SQLiteConnection connection = null;

	/**
	 * Instantiates a new instrument config reader.
	 * 
	 * @param connection
	 *            the connection
	 */
	public InstrumentConfigReader(SQLiteConnection connection) {
		super();
		this.connection = connection;
	}

	/**
	 * Gets the instrument config.
	 * 
	 * @param id
	 *            the id
	 * @return the instrument config
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public List<InstrumentConfiguration> getInstrumentConfigs() throws SQLiteException {
	   List<InstrumentConfiguration> l = new ArrayList<InstrumentConfiguration>();
		 SQLiteRecordIterator it = new SQLiteQuery(connection, "select * from instrument_configuration").getRecords();
		 while( it.hasNext()) {
       SQLiteRecord r =  it.next(); 
		   int id = r.columnInt(InstrumentConfigurationTable.ID);
       String name = r.columnString(InstrumentConfigurationTable.NAME);
       int softwareId = r.columnInt(InstrumentConfigurationTable.SOFTWARE_ID);
       String paramTreeAsStr = r.columnString(InstrumentConfigurationTable.PARAM_TREE);
       String componentListAsStr = r.columnString(InstrumentConfigurationTable.COMPONENT_LIST);
       InstrumentConfiguration insConf = new InstrumentConfiguration(id, name, softwareId, 
           ParamTreeParser.parseParamTree(paramTreeAsStr), ParamTreeParser.parseComponentList(componentListAsStr));
       l.add(insConf);
		 }
		 return l;
	}
	
	
	public InstrumentConfiguration getInstrumentConfig(int id) throws SQLiteException {
    return new SQLiteQuery(connection, "select * from instrument_configuration where id = ?").bind(1, id)
        .extractRecord(new ISQLiteRecordExtraction<InstrumentConfiguration>() {
                public InstrumentConfiguration extract(SQLiteRecord r) throws SQLiteException {
                        int id = r.columnInt(InstrumentConfigurationTable.ID);
                        String name = r.columnString(InstrumentConfigurationTable.NAME);
                        int softwareId = r.columnInt(InstrumentConfigurationTable.SOFTWARE_ID);
                        String paramTreeAsStr = r.columnString(InstrumentConfigurationTable.PARAM_TREE);
                        String insConfAsStr = r.columnString(InstrumentConfigurationTable.COMPONENT_LIST);
                        return new InstrumentConfiguration(id, name, softwareId, ParamTreeParser.parseParamTree(paramTreeAsStr),
                                        ParamTreeParser.parseComponentList(insConfAsStr));

                }
        });

	}

}
