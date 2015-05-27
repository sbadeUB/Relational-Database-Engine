package edu.buffalo.cse562;
import java.io.*;
import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromScanner2 implements FromItemVisitor {
	//Input values
	File basePath;
	HashMap<String, CreateTable> tables;
	List<Expression> selectExpressions =new ArrayList<Expression>();
	ArrayList<String> fullcolumnNames;
	ArrayList<String> columnNames;
	Operator source=null;
	public FromScanner2(File basePath,List<Expression> seleExpressions,HashMap<String, CreateTable> tables) {
		this.basePath=basePath;
		this.tables=tables;
		this.selectExpressions=seleExpressions;
	}
	
	public Expression getExpressionForTable(String tablename)
	{
		int count=0;
		Expression expr = null;
		for(int i=0;i<selectExpressions.size();i++)
		{
			if(selectExpressions.get(i).toString().contains(tablename))
			{
				count=count+1;
				if(count==1)
				{
					expr=selectExpressions.get(i);
				}
				else if(count>1)
				{
					expr=new AndExpression(expr, selectExpressions.get(i));
				}
				//mainOper= new SelectionOperator(frmScanner.source, frmScanner.fullcolumnNames, frmScanner.columnNames, selectexpressions.get(i));
			}
			if(!(selectExpressions.get(i).toString().contains(".") )){
				count++;
				expr=selectExpressions.get(i);
			}

		}
		if(count>0) return expr;
		return null;
	}
	@Override
	public void visit(Table tablename) {
		CreateTable table=tables.get(tablename.getName().toLowerCase());
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> cols=table.getColumnDefinitions();
		fullcolumnNames=new ArrayList<String>();
		columnNames=new ArrayList<String>();
		for(int i=0;i<cols.size();i++)
		{
			ColumnDefinition col=(ColumnDefinition)(cols.get(i));
			if(tablename.getAlias() != null)
				fullcolumnNames.add(tablename.getAlias().toLowerCase()+"."+col.getColumnName().toLowerCase());
			else 
				fullcolumnNames.add(tablename.getName().toLowerCase()+"."+col.getColumnName().toLowerCase());
				columnNames.add(col.getColumnName().toLowerCase());
		}
		String tblname=tablename.getName().toLowerCase();
		Expression expr=getExpressionForTable(tblname);
		source=new OptimizedScanOperator(new File(basePath, tablename.getName()+".dat"),fullcolumnNames, columnNames,table,expr);
	}
	
	@Override
	public void visit(SubSelect subselect) {
	}

	@Override
	public void visit(SubJoin arg0) {

	}

}
