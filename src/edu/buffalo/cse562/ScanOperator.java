/*package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ScanOperator implements Operator {
	
	BufferedReader input;
	File f;
	CreateTable table;
	ArrayList<ColDataType> tabledatatypeslist=new ArrayList<ColDataType>();
	
	

	public ScanOperator(File f,CreateTable table)
	{
		this.f=f;
		this.table=table;
		reset();
		getColumnDatatypes();
	}
	
	public void getColumnDatatypes()
	{
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> colDefs= this.table.getColumnDefinitions(); 
		for(int i1=0;i1<colDefs.size();i1++)
			this.tabledatatypeslist.add(colDefs.get(i1).getColDataType());
	}
	
	@Override
	public Datum[] readOneTuple() {
		if(input==null) return null;
		String line=null;
		try {
			line=input.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//line=line.trim();
		if(line==null || line==""){
			try {
				input.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
		String[] cols=line.split("\\|"); 
		Datum[] ret=new Datum[cols.length];
		
		for(int i=0;i<cols.length;i++)
		{
			String coldatatype=tabledatatypeslist.get(i).toString();
			coldatatype=coldatatype.toLowerCase();
			if(coldatatype.startsWith("char") || coldatatype.startsWith("varchar"))
				coldatatype="string";
			switch(coldatatype)
			{
				case "int":
				{
					ret[i]=new Datum.DatumLong(cols[i]);				
					break;
				}
				case "decimal":
				{
					ret[i]=new Datum.DatumDecimal(cols[i]);
					break;
				}
				case "string":
				{
					ret[i]=new Datum.DatumString(cols[i]);
					break;
				}
				case "date":
				{
					ret[i]=new Datum.DatumDate(cols[i]);
					break;
				}
				default:
				{
					try {
						throw new Exception("UnRecognisedDataTypeException:Datatype:"+coldatatype);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		
		return ret;
	}

	@Override
	public void reset() {
		try {
			this.input=new BufferedReader(new FileReader(f));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			input=null;
		}
	}

}
*/