package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class OptimizedScanOperator implements Operator {

	BufferedReader input;
	File f;
	String[] coldatatypes=null;
	ArrayList<String> fullColumnnames=new ArrayList<String>();
	ArrayList<String> Columnnames=new ArrayList<String>();
	Expression expr;
	public OptimizedScanOperator(File f,ArrayList<String> fullColumnnames,ArrayList<String> Columnnames,CreateTable table,Expression expr)
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
		if(input==null) return null;
		String line=null;
		try {
			line=input.readLine();
			if(line==null || line==""){
				input.close();
				return null;
			}
		} catch (IOException exc) {
			exc.printStackTrace();
		}
		//Datum[] tuple=null;
		Tuple tuple=null;
		do{
			tuple=new Tuple(line, this.coldatatypes);
			Evaluator eval=new Evaluator(fullColumnnames,Columnnames,tuple);
			if(expr!=null){
				expr.accept(eval);
				if(!eval.getBool()) tuple=null;
				else return tuple;
				try {
					line=input.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
				if(line==null) return null;
			}
			else return tuple;
		}while(tuple==null);

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
