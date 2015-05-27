/*package edu.buffalo.cse562;
import java.io.*;
import java.util.*;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromScanner implements FromItemVisitor {
	//Input values
	File basePath;
	HashMap<String, CreateTable> tables;
	//Returning values
	ArrayList<String> fullcolumnNames;
	ArrayList<String> columnNames;
	Operator source=null;
	public FromScanner(File basePath, HashMap<String, CreateTable> tables) {
		this.basePath=basePath;
		this.tables=tables;
	}
	@Override
	public void visit(Table tablename) {
		CreateTable table=tables.get(tablename.getName().toLowerCase());
		//System.out.println("Table:"+tablename+"::CreateTable:"+table.getColumnDefinitions());
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> cols=table.getColumnDefinitions();
		fullcolumnNames=new ArrayList<String>();
		columnNames=new ArrayList<String>();
		String aliasname=null;
		if(tablename.getAlias()!=null){			
		 aliasname=tablename.getAlias().toLowerCase();
		}
		String tblename=tablename.getName().toLowerCase();
		for(int i=0;i<cols.size();i++)
		{
			ColumnDefinition col=(ColumnDefinition)(cols.get(i));
			if(aliasname != null)
				fullcolumnNames.add(aliasname+"."+col.getColumnName().toLowerCase());
			else 
				fullcolumnNames.add(tblename+"."+col.getColumnName().toLowerCase());
				columnNames.add(col.getColumnName().toLowerCase());
		}
		source=new ScanOperator(new File(basePath, tablename.getName()+".dat"), table);
	}
	
	@Override
	public void visit(SubSelect subselect) {
	}

	@Override
	public void visit(SubJoin arg0) {

	}

}
*/