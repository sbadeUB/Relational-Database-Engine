/*package edu.buffalo.cse562;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map.Entry;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class Indexer {
	File dbDir=null;
	File currDataFile=null;
	File dataDir=null;
	HashMap<String, Integer> primaryIndexes=new HashMap<String, Integer>();
	HashMap<String, String[]> SecondaryIndexes=new HashMap<String, String[]>();
	//Primary Index on LINEITEM, ORDERS, CUSTOMER, SUPPLIER, NATION, REGION
	//lineitem:10:shipdate,4:quantity,6:discount,8:returnflag,11:commitdate,12:receiptdate,14:shipmode
	//customer:6:mktsegment
	//orders:4:orderdate
	//region:1:name
	public Indexer(File dbDir,File dataDir,HashMap<String, CreateTable> tables) {	
		this.dbDir=dbDir;
		this.dataDir=dataDir;
		for(Entry<String,CreateTable> entry : tables.entrySet()){	
			primaryIndexes.put(entry.getKey(), 0);
		}
		for(Entry<String,CreateTable> entry : tables.entrySet()){	
			String tablename=entry.getKey();
			switch(tablename)
			{
				case "lineitem":
				{
					String[] secondarycolumns=new String[7];
					secondarycolumns[0]="10:shipdate";
					secondarycolumns[1]="4:quantity";
					secondarycolumns[2]="6:discount";
					secondarycolumns[3]="8:returnflag";
					secondarycolumns[4]="11:commitdate";
					secondarycolumns[5]="12:receiptdate";
					secondarycolumns[6]="14:shipmode";
					SecondaryIndexes.put("lineitem", secondarycolumns);				
					break;
				}
				case "customer":
				{
					String[] secondarycolumns=new String[1];
					secondarycolumns[0]="6:mktsegment";
					SecondaryIndexes.put("customer", secondarycolumns);
					break;
				}
				case "orders":
				{
					String[] secondarycolumns=new String[1];
					secondarycolumns[0]="4:orderdate";
					SecondaryIndexes.put("orders", secondarycolumns);
					break;
				}
				case "region":
				{
					String[] secondarycolumns=new String[1];
					secondarycolumns[0]="1:name";
					SecondaryIndexes.put("region", secondarycolumns);
					break;
				}
				default:
				{
					//No secondary index on this table
					break;
				}
			}
		}
	}

	public void putData(String key, String value,Database myDatabase){
		String aKey = key;
		String aData = value;

		try {
			DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
			DatabaseEntry theData = new DatabaseEntry(aData.getBytes("UTF-8"));
			myDatabase.put(null, theKey, theData);
		} catch (Exception e) {
			// Exception handling goes here
		}
	}

	public void getData(String aKey,Database myDatabase){

		try {
			// Create a pair of DatabaseEntry objects. theKey
			// is used to perform the search. theData is used
			// to store the data returned by the get() operation.
			DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
			DatabaseEntry theData = new DatabaseEntry();

			// Perform the get.
			if (myDatabase.get(null, theKey, theData, LockMode.DEFAULT) ==
					OperationStatus.SUCCESS) {

				// Recreate the data String.
				byte[] retData = theData.getData();
				String foundData = new String(retData, "UTF-8");
				System.out.println("For key: '" + aKey + "' found data: '" + 
						foundData + "'.");
			} else {
				System.out.println("No record found for key '" + aKey + "'.");
			}  
		} catch (Exception e) {
			// Exception handling goes here
		}
	}
	public void putTuple(String key, Tuple value,Database myDatabase){
		String aKey = key;
		try{
			DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			ObjectOutputStream objOut=new ObjectOutputStream(out);
			objOut.writeObject(value);
			byte[] tupleData=out.toByteArray();
			DatabaseEntry theData = new DatabaseEntry(tupleData);
			myDatabase.put(null, theKey, theData);

		}catch(Exception e){
			e.printStackTrace();
		} 
	}

	public Tuple getTuple(String key, Database myDatabase){
		String aKey = key;
		try{
			DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
			DatabaseEntry theData = new DatabaseEntry();
			if (myDatabase.get(null, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS)
			{
				// Recreate the data String.
				byte[] retData = theData.getData();
				ByteArrayInputStream in=new ByteArrayInputStream(retData);
				ObjectInputStream objIn=new ObjectInputStream(in);
				Tuple t=(Tuple) objIn.readObject();
				return t;
			}

		}catch(Exception e){
			e.printStackTrace();
		} 
		return null;
	}
	public void deleteData(String aKey,Database myDatabase){
		try {
			DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));

			// Perform the deletion. All records that use this key are
			// deleted.
			myDatabase.delete(null, theKey); 
		} catch (Exception e) {
			// Exception handling goes here
		}
	}
	
	public class MyKeyCreator implements SecondaryKeyCreator {
		private int secondaryIndexPosition=-1;
		public MyKeyCreator(int secIndexPos){
			this.secondaryIndexPosition=secIndexPos;
		}
		 
	    public boolean createSecondaryKey(SecondaryDatabase secondaryDB,
	                                      DatabaseEntry keyEntry, 
	                                      DatabaseEntry dataEntry,
	                                      DatabaseEntry resultEntry) {
	        try {
	        	byte[] retData = dataEntry.getData();
	        	ByteArrayInputStream in=new ByteArrayInputStream(retData);
				ObjectInputStream objIn=new ObjectInputStream(in);
				Tuple t=(Tuple) objIn.readObject();
				String secKey=t.data[secondaryIndexPosition];
				resultEntry= new DatabaseEntry(secKey.getBytes("UTF-8"));
	        } catch (IOException | ClassNotFoundException willNeverOccur) {}
	        return true;
	    }
	} 

	public boolean indexTables() {
		Environment myDbEnvironment = null;
		Database myDatabase = null;
		BufferedReader input=null;
		SecondaryDatabase mySecDB=null;
		
		try {
			// Open the environment. Create it if it does not already exist.
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			String externalPath = System.getProperty("user.dir")+File.separator+dbDir;
			myDbEnvironment = new Environment(new File(externalPath), envConfig);

			// Open the database. Create it if it does not already exist.
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true); 			
			
			//Iterate over all table Indexes
			for(Entry<String,Integer> entry : primaryIndexes.entrySet()){		
				currDataFile=new File(dataDir, entry.getKey()+".dat");
				myDatabase = myDbEnvironment.openDatabase(null, entry.getKey(), dbConfig);
				try {
					input=new BufferedReader(new FileReader(currDataFile));
					String dataLine=null;
					while((dataLine=input.readLine())!=null){
						Tuple tupleValue=new Tuple(dataLine);
						String tuplekey=tupleValue.data[entry.getValue()]; //index depends on the position of Key let it be "0"
						putTuple(tuplekey, tupleValue, myDatabase);
					}
					if(SecondaryIndexes.containsKey(entry.getKey()))
					{
						String[] secondaryindexset=SecondaryIndexes.get(entry.getKey());
						for(String s:secondaryindexset)
						{
							String[] splitindex=s.split(":");
							int indexPos=Integer.parseInt(splitindex[0]);
							String secondaryDBName=splitindex[1];
							SecondaryConfig mySecConfig = new SecondaryConfig();
						    mySecConfig.setKeyCreator(new MyKeyCreator(indexPos));
						    mySecDB=myDbEnvironment.openSecondaryDatabase(null, secondaryDBName, myDatabase, mySecConfig);
						}						
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			}
			
		} catch (DatabaseException dbe) {
			dbe.printStackTrace();
			return false;
		}
		finally{
			if(mySecDB!=null){
				mySecDB.close();
			}
			if(input!=null){
				try {
					input.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			}
			if (myDatabase != null) {
				myDatabase.close();
			}

			if (myDbEnvironment != null) {
				myDbEnvironment.close();
			}
		}
		return true;
	}

}
*/