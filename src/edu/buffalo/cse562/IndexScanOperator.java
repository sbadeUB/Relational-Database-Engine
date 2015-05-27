/*package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class IndexScanOperator implements Operator {
	BufferedReader input;
	File f;
	String[] coldatatypes=null;
	ArrayList<String> fullColumnnames=new ArrayList<String>();
	ArrayList<String> Columnnames=new ArrayList<String>();
	Expression expr;
	public IndexScanOperator(File f,ArrayList<String> fullColumnnames,ArrayList<String> Columnnames,CreateTable table,Expression expr)
	{
		this.f=f;
		this.fullColumnnames=fullColumnnames;
		this.Columnnames=Columnnames;
		this.expr=expr;
		reset();
		getColumnDatatypes(table);
	}

	public void getColumnDatatypes(CreateTable table)
	{
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> colDefs= table.getColumnDefinitions(); 
		coldatatypes=new String[colDefs.size()]; 
		for(int i1=0;i1<colDefs.size();i1++){			
			String coldatatype=colDefs.get(i1).getColDataType().toString();
			coldatatype=coldatatype.toLowerCase();
			if(coldatatype.startsWith("char") || coldatatype.startsWith("varchar"))
				coldatatypes[i1]="string";
			else coldatatypes[i1]=coldatatype;
		}
	}

	
	@Override
	public Tuple readOneTuple() {
		
		return null;
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