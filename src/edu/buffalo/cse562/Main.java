package edu.buffalo.cse562;
import java.io.*;
import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		int i;
		File dataDir=null;
		File swapdir=null;
		ArrayList<File> sqlFiles=new ArrayList<File>();
		HashMap<String,CreateTable> tables=new HashMap<String,CreateTable>();
		System.currentTimeMillis();
		for(i=0;i<args.length;i++)
		{
			if(args[i].equals("--data"))
			{
				i++;
				dataDir=new File(args[i]);
				
				
			}
			else if(args[i].equals("--db"))
			{
				i++;
				new File(args[i]);
			}
			else if(args[i].equals("--load"))
			{
				i++;
				new File(args[i]);
			}
			else if(args[i].equals("--swap"))
			{
				i++;
				swapdir=new File(args[i]);
			}
			else
			{
				sqlFiles.add(new File(args[i]));
			}
		}
		System.gc();
		for(File sql:sqlFiles)
		{
			try {
				FileReader fr=new FileReader(sql);
				CCJSqlParser parser=new CCJSqlParser(fr);
				Statement stmt=null;
				while((stmt=parser.Statement())!=null)
				{
					if(stmt instanceof CreateTable)
					{
						CreateTable ct=(CreateTable)stmt;
						tables.put(ct.getTable().getName().toLowerCase(),ct);
					}
					else if(stmt instanceof Select)
					{
						SelectBody select=((Select)stmt).getSelectBody();
						OperatorSchemasContainer containerObject;
						Operator mainOper = null;
						if(select instanceof PlainSelect)
						{
							containerObject=executePlainSelectPlan(select,dataDir,swapdir,tables);
							mainOper=containerObject.oper;
						}
						else if(select instanceof Union)
						{
							Union unionStmts=(Union)select;
							List<SelectBody> plainSelectStmts=unionStmts.getPlainSelects();
							List<Operator> operatorsList=new ArrayList<Operator>();
							ArrayList<String> mainSchema=new ArrayList<String>();
							ArrayList<String> schema=new ArrayList<String>();
							for(SelectBody selectStmt:plainSelectStmts)
							{
								Operator oper=null;
								if(selectStmt instanceof PlainSelect)
								{
									containerObject=executePlainSelectPlan(selectStmt,dataDir,swapdir,tables);
									oper=containerObject.oper;
									mainSchema=containerObject.fullSchema;
									schema=containerObject.schema;
								}
								operatorsList.add(oper);
							}
							mainOper=new UnionOperator(operatorsList,mainSchema,schema);
						}
						//System.out.println("Select:"+select);
						//System.out.println("Printing:");
						DumpOutput(mainOper);
						//System.out.println("done!");
					}
					else
					{
						System.out.println("Error:Other Statement types not Handled!");
					}

				}
			} catch (IOException | ParseException e) {
				e.printStackTrace();
			}
		}
		System.currentTimeMillis();
	}

	

	@SuppressWarnings({ "unchecked" })
	public static OperatorSchemasContainer executePlainSelectPlan(SelectBody select,File dataDir,File swapdir,HashMap<String, CreateTable> tables) 
	{		

		PlainSelect pselect=(PlainSelect)select;
		FromScanner2 frmScanner;
		ArrayList<String> mainFullSchema=null;
		ArrayList<String> mainSchema=null;
		Operator mainOper=null;
		boolean isSubSelect=false;
		List<Expression> selectexpressions= new ArrayList<Expression>();
		List<Expression> joinexpressions= new ArrayList<Expression>();
		List<Expression> expressions= new ArrayList<>();
		List<Expression> lessthanjoinexpressions= new ArrayList<Expression>();
		FromItem fromItem=pselect.getFromItem();
		if(pselect.getJoins()!=null) 	
		{
		}
		if(pselect.getWhere()!=null)
		{
			if(pselect.getWhere() instanceof AndExpression){
				AndExpression e=(AndExpression)pselect.getWhere();
				expressions=splitAndClauses(e);
				for(int i=0;i<expressions.size();i++)
				{
					if(!(expressions.get(i).toString().contains("=")))
					{
						String[] expressionsplit= new String[2];
						if(expressions.get(i).toString().contains("<"))
							expressionsplit=expressions.get(i).toString().split("<");

						if(expressions.get(i).toString().contains(">"))
							expressionsplit=expressions.get(i).toString().split(">");

						if(expressions.get(i).toString().contains(">="))
							expressionsplit=expressions.get(i).toString().split(">=");				 


						if((expressionsplit[0].contains(".")) && (expressionsplit[1].contains(".")))
						{
							String[] tablenameandcolumn1=expressionsplit[0].split("\\.");
							String[] tablenameandcolumn2=expressionsplit[0].split("\\.");
							if(tablenameandcolumn1[0].trim().equals(tablenameandcolumn2[0].trim()))
							{
								selectexpressions.add(expressions.get(i));
							}
							else
								lessthanjoinexpressions.add(expressions.get(i));
						}
						else
							selectexpressions.add(expressions.get(i));
					}
					if(expressions.get(i).toString().contains("="))
					{
						if(expressions.get(i).toString().contains("OR"))
						{
							selectexpressions.add(expressions.get(i));;
							continue;
						}

						String[] conds=expressions.get(i).toString().split("=");
						if(conds[1].contains(".") && conds[0].contains("."))
						{

							joinexpressions.add(expressions.get(i));
						}
						else
						{
							selectexpressions.add(expressions.get(i));
						}
					}
				}
			}
			else{
				selectexpressions.add(pselect.getWhere());
			}
		}
		frmScanner=new FromScanner2(dataDir,selectexpressions,tables);

		if(fromItem instanceof SubSelect)
		{
			isSubSelect=true;
			OperatorSchemasContainer opSchema;
			opSchema=executeSubSelect(fromItem,dataDir,swapdir,tables);
			mainOper=opSchema.oper;
			mainFullSchema=opSchema.fullSchema;
			mainSchema=opSchema.schema;
		}

		if(!isSubSelect)
		{
			fromItem.accept(frmScanner);
			mainOper=frmScanner.source;
			mainFullSchema=frmScanner.fullcolumnNames;
			mainSchema=frmScanner.columnNames;
		}
		isSubSelect=false;
		if(pselect.getJoins()!=null)
		{
			List<Join> joins= pselect.getJoins();
			boolean middlejoin= false;
			for(int i1=0;i1<joins.size();i1++)
			{ 
				Operator rightOper=null;
				ArrayList<String> tempMainSchema=null;
				ArrayList<String> tempSchema=null;
				FromItem joinItem=joins.get(i1).getRightItem();
				if(joinItem instanceof SubSelect)
				{
					isSubSelect=true;
					OperatorSchemasContainer opSchema;
					opSchema=executeSubSelect(joinItem,dataDir,swapdir,tables);
					mainOper=opSchema.oper;
					mainFullSchema=opSchema.fullSchema;
					mainSchema=opSchema.schema;
					if(mainFullSchema==null) mainFullSchema=((UnionOperator)mainOper).mainSchema;
					if(mainSchema==null) mainSchema=((UnionOperator)mainOper).schema;
				}
				if(!isSubSelect)
				{
					joinItem.accept(frmScanner);
					rightOper=frmScanner.source;
					tempMainSchema=frmScanner.fullcolumnNames;
					tempSchema=frmScanner.columnNames;
				}
				isSubSelect=false;
				mainFullSchema.addAll(tempMainSchema);
				mainSchema.addAll(tempSchema);
				String[] tablescoloumns= new String[2];
				Expression tobesent= new EqualsTo();

				if(!middlejoin)
				{
					for(int i=0;i<joinexpressions.size();i++)
					{
						tablescoloumns=joinexpressions.get(i).toString().split("=");
						if((mainFullSchema.contains(tablescoloumns[0].trim())) && (mainFullSchema.contains(tablescoloumns[1].trim())) )
						{
							tobesent=joinexpressions.get(i);
							joinexpressions.remove(i);
							middlejoin= true;
							break;
						}
					}
				}

				if(!middlejoin){
					mainOper= new InMemoryHybridHashJoin(mainFullSchema,mainSchema, mainOper,rightOper, joins.get(i1).getOnExpression());
				}
				else{
					if(swapdir!=null)
						mainOper= new InMemoryHybridHashJoin(mainFullSchema,mainSchema, mainOper,rightOper, tobesent);
					else
						mainOper= new InMemoryHybridHashJoin(mainFullSchema,mainSchema, mainOper,rightOper, tobesent);
					middlejoin= false;
				}
			}
		}
		/*if(lessthanjoinexpressions.size()>0)
		{
			Expression lessthanjoin=lessthanjoinexpressions.get(0);
			for(int i=1;i<lessthanjoinexpressions.size();i++)
			{
				lessthanjoin= new AndExpression(lessthanjoin, expressions.get(i));
			}
			mainOper=new SelectionOperator(mainOper, mainFullSchema,mainSchema,lessthanjoin);
		}*/
		if(joinexpressions.size()>0){
			mainOper=new SelectionOperator(mainOper, mainFullSchema,mainSchema,joinexpressions.get(0));  
		}

		List<SelectItem> selectItemList = null;
		boolean isAllColumnsSet=false;
		boolean isAggrExprsFound=false;
		ArrayList<Expression> aggOperationExprs=new ArrayList<Expression>();
		if(pselect.getSelectItems()!=null)
		{
			selectItemList=pselect.getSelectItems();
			for(SelectItem x:selectItemList)
			{
				if(x instanceof AllColumns)
				{
					isAllColumnsSet=true;
					break;
				}
				else if(x instanceof AllTableColumns)
				{
				}
				else if(x instanceof SelectExpressionItem)
				{
					if(((SelectExpressionItem) x).getAlias()==null)
					{
						if(((SelectExpressionItem) x).getExpression() instanceof Function) {
							String tmp="internal_aggr_"+aggOperationExprs.size();
							((SelectExpressionItem) x).setAlias(tmp);
							aggOperationExprs.add((((SelectExpressionItem) x).getExpression()));
							isAggrExprsFound=true;
						} else {
							((SelectExpressionItem) x).setAlias((((SelectExpressionItem) x).getExpression()).toString());
						}
					}
					else
					{
						if(((SelectExpressionItem) x).getExpression() instanceof Function) {
							aggOperationExprs.size();
							aggOperationExprs.add((((SelectExpressionItem) x).getExpression()));
							isAggrExprsFound=true;
						}
						else {
							//No operation Since Alias:Expression found
						}	
					}
				}
			}
		}
		mainOper=new ProjectionOperator(mainOper, selectItemList,mainFullSchema,mainSchema);
		if(((ProjectionOperator)mainOper).finalSchema.size()==0)
		{
			((ProjectionOperator)mainOper).buildSchemas();
			mainFullSchema=((ProjectionOperator)mainOper).fullFinalSchema;
			mainSchema=((ProjectionOperator)mainOper).finalSchema;
		}
		boolean issortedinGroupBy=false;
		if(pselect.getGroupByColumnReferences()!=null && !isAllColumnsSet) //If groupBy Columns exists
		{
			List<Column> groupByList=pselect.getGroupByColumnReferences();
			Expression havingExpr=null;
			if(pselect.getHaving()!=null)
			{
				havingExpr=pselect.getHaving();
			}
			mainOper=new GroupByOperator2(mainOper, selectItemList, groupByList,mainFullSchema,mainSchema,havingExpr);
			Tuple outTuple=mainOper.readOneTuple();
			List<Tuple> outTupleList=new ArrayList<Tuple>();
			while(outTuple!=null)
			{
				outTuple=mainOper.readOneTuple();			
			}
			outTupleList=((GroupByOperator2)mainOper).getOutputList();
			if(((GroupByOperator2)mainOper).finalSchema.size()==0)
			{
				((GroupByOperator2)mainOper).buildSchemas();
			}
			mainOper=new ScanListOperator(outTupleList, ((GroupByOperator2)mainOper).fullFinalSchema, ((GroupByOperator2)mainOper).finalSchema);
			mainFullSchema=((ScanListOperator)mainOper).fullColumnNames;
			mainSchema=((ScanListOperator)mainOper).columnNames;

		}
		else if(isAggrExprsFound && !isAllColumnsSet)
		{
			mainOper=new AggregateOperator(mainOper, selectItemList,mainFullSchema,mainSchema);
			Tuple outTuple=mainOper.readOneTuple();
			List<Tuple> outTupleList=new ArrayList<Tuple>();
			while(outTuple!=null)
			{
				outTuple=mainOper.readOneTuple();			
			}
			outTuple=((AggregateOperator)mainOper).getOutput();
			outTupleList.add(outTuple);
			if(((AggregateOperator)mainOper).finalSchema.size()==0)
			{
				((AggregateOperator)mainOper).buildSchemas();
			}
			mainOper=new ScanListOperator(outTupleList, ((AggregateOperator)mainOper).fullFinalSchema, ((AggregateOperator)mainOper).finalSchema);
		}
		else if(!isAllColumnsSet)
		{
			mainOper=new ProjectionOperator(mainOper, selectItemList,mainFullSchema,mainSchema);
			if(((ProjectionOperator)mainOper).finalSchema.size()==0)
			{
				((ProjectionOperator)mainOper).buildSchemas();
				mainFullSchema=((ProjectionOperator)mainOper).fullFinalSchema;
				mainSchema=((ProjectionOperator)mainOper).finalSchema;
			}
		}
		if(pselect.getOrderByElements()!=null)
		{
			if(!issortedinGroupBy){
				List<OrderByElement> orderelements= new ArrayList<OrderByElement>();
				orderelements = pselect.getOrderByElements();
				int limit=-1;
				if(pselect.getLimit()!=null)
				{
					limit=(int) pselect.getLimit().getRowCount();
				}
				mainOper =new SortOperator(mainOper, mainFullSchema,mainSchema,orderelements,limit);
				Tuple outTuple=mainOper.readOneTuple();
				List<Tuple> outTupleList=new ArrayList<Tuple>();
				while(outTuple!=null)
				{
					outTuple=mainOper.readOneTuple();			
				}
				outTupleList=((SortOperator)mainOper).getOutputList();
				mainOper=new ScanListOperator(outTupleList, ((SortOperator)mainOper).mainSchema, ((SortOperator)mainOper).schema);
			}
		}
		OperatorSchemasContainer containerObject=new OperatorSchemasContainer(mainOper, mainFullSchema, mainSchema);

		return containerObject;
	}
	
	public static OperatorSchemasContainer executeSubSelect(FromItem fromItem,File dataDir,File swapdir,HashMap<String,CreateTable> tables)
	{
		Operator mainOper = null;
		ArrayList<String> mainSchema=new ArrayList<String>();
		ArrayList<String> schema=new ArrayList<String>();
		OperatorSchemasContainer containerObject;
		SelectBody selectBody=((SubSelect) fromItem).getSelectBody();
		if(selectBody instanceof PlainSelect)
		{
			containerObject=executePlainSelectPlan(selectBody, dataDir,swapdir,tables);
			mainOper=containerObject.oper;
			mainSchema=containerObject.fullSchema;
			schema=containerObject.schema;
		}
		else if(selectBody instanceof Union)
		{
			Union unionStmts=(Union)selectBody;
			@SuppressWarnings("unchecked")
			List<SelectBody> plainSelectStmts=unionStmts.getPlainSelects();
			List<Operator> operatorsList=new ArrayList<Operator>();
			for(SelectBody selectStmt:plainSelectStmts)
			{
				Operator oper=null;
				if(selectStmt instanceof PlainSelect)
				{
					containerObject=executePlainSelectPlan(selectStmt,dataDir,swapdir,tables);
					oper=containerObject.oper;
					mainSchema=containerObject.fullSchema;
					schema=containerObject.schema;
				}
				operatorsList.add(oper);
			}
			mainOper=new UnionOperator(operatorsList,mainSchema,schema);
		}

		//System.out.println("SubSelect:"+selectBody);
		containerObject=new OperatorSchemasContainer(mainOper, mainSchema, schema);
		return containerObject;
	}

	public static List<Expression> splitAndClauses(Expression e) 
	{
		List<Expression> ret = new ArrayList<Expression>();
		if(e instanceof AndExpression){
			AndExpression a = (AndExpression)e;
			ret.addAll(
					splitAndClauses(a.getLeftExpression())
					);
			ret.addAll(
					splitAndClauses(a.getRightExpression())
					);
		} else {
			ret.add(e);
		}
		return ret;
	}

	public static void DumpOutput(Operator input)
	{
		Tuple outTuple=input.readOneTuple();	
		while(outTuple!=null)
		{
			for(int i=0;i<outTuple.gettupledata().length;i++)
			{
				if(i!=(outTuple.gettupledata().length-1))
					System.out.print(outTuple.gettupledata()[i].toString().replaceAll("^\'|\'$", "")+"|");	
				else
					System.out.print(outTuple.gettupledata()[i].toString().replaceAll("^\'|\'$", ""));
			}
			System.out.println("");
			outTuple=input.readOneTuple();			
		}
	}
}

