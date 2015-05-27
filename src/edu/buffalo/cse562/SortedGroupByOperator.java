/*package edu.buffalo.cse562;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Datum.DatumDate;
import edu.buffalo.cse562.Datum.DatumDecimal;
import edu.buffalo.cse562.Datum.DatumLong;
import edu.buffalo.cse562.Datum.DatumString;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SortedGroupByOperator implements Operator {
	
	Operator input;
	List<SelectItem> selectItemslist;
	List<Column> groupByList;
	ArrayList<String> fullSchema;
	ArrayList<String> schema;
	ArrayList<String> havingSchema=new ArrayList<String>();
	List<Datum[]> resultTuplesList=new ArrayList<Datum[]>();
	List<Datum> outTuple;
	String groupByColumnFormation="";
	long runningCount=0;
	boolean isFirstTuple;
	Expression havingExpr;
	ArrayList<String> fullFinalSchema=new ArrayList<String>();
	ArrayList<String> finalSchema=new ArrayList<String>();
	
	public SortedGroupByOperator(Operator oper,List<SelectItem> selectItemList,
							List<Column> groupByList,ArrayList<String> schema,ArrayList<String> schema2,Expression havingExpr)	{
		this.input=oper;
		this.selectItemslist=selectItemList;
		this.groupByList=groupByList;
		this.fullSchema=schema;
		this.schema=schema2;
		this.isFirstTuple=true;
		this.havingExpr=havingExpr;
		checkForHavingSchema();
	}

	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple=null;
		tuple=input.readOneTuple();
		if(tuple==null){ 
			if(outTuple!=null){
				runningCount=0;
				Datum[] tupleArray=new Datum[outTuple.size()];
				tupleArray=  outTuple.toArray(tupleArray);
				applyHavingFilter(tupleArray);
				outTuple=null;
			}
			return tuple;
		}
		else
		{
			String formedGroupByTuple="";
			for(int i=0;i<groupByList.size();i++)
			{
				Column targetColumn=groupByList.get(i);
				int colID = -1;
				if(targetColumn.getTable().getAlias()!=null)
				{
					if(fullSchema.contains(targetColumn.getTable().getAlias().toLowerCase()+"."+targetColumn.getColumnName().toLowerCase()))
					{
						colID = fullSchema.indexOf(targetColumn.getTable().getAlias().toLowerCase()+"."+targetColumn.getColumnName().toLowerCase());
					}
				}
				else if(targetColumn.getTable().getName()!=null)
				{
					if(fullSchema.contains(targetColumn.getTable().getName().toLowerCase()+"."+targetColumn.getColumnName().toLowerCase()))
					{
						colID = fullSchema.indexOf(targetColumn.getTable().getName().toLowerCase()+"."+targetColumn.getColumnName().toLowerCase());
					}
				}
				else if(fullSchema.contains(targetColumn.getColumnName().toLowerCase()))
				{
					colID = fullSchema.indexOf(targetColumn.getColumnName().toLowerCase());
				}
				else
				{
					if(schema.contains(targetColumn.getColumnName().toLowerCase()))
					{
						colID = schema.indexOf(targetColumn.getColumnName().toLowerCase());
					}
				}
				if(colID!=-1)
				formedGroupByTuple+=(tuple[colID].toString())+":";
				else System.out.println("Sorry!,Wrong GroupByColumn found in GroupBy Clause!");
			}
			
			
			if(isFirstTuple) {
				outTuple=processProjectListPerTuple(tuple,formedGroupByTuple,null,0L);
				groupByColumnFormation=formedGroupByTuple;
				runningCount++;
				isFirstTuple=false;
			}
			else if(groupByColumnFormation.equals(formedGroupByTuple)){
				outTuple=processProjectListPerTuple(tuple,formedGroupByTuple,outTuple,runningCount);
				runningCount++;
			}
			else{
				runningCount=0;
				Datum[] tupleArray=new Datum[outTuple.size()];
				tupleArray=  outTuple.toArray(tupleArray);
				applyHavingFilter(tupleArray);
				isFirstTuple=true;
				outTuple=processProjectListPerTuple(tuple,formedGroupByTuple,null,0L);
				groupByColumnFormation=formedGroupByTuple;
				runningCount++;
				isFirstTuple=false;
			}
		}
		return tuple;
	}
	
	public void buildSchemas()
	{
		for(int i=0;i<selectItemslist.size();i++)
		{
			if(selectItemslist.get(i) instanceof SelectExpressionItem)
			{
				SelectExpressionItem st=((SelectExpressionItem) selectItemslist.get(i));
				if(st.getAlias()!=null)
				{
					fullFinalSchema.add(st.getAlias().toLowerCase());
				}
					finalSchema.add(st.getExpression().toString().toLowerCase());
			}
			
			else if(selectItemslist.get(i) instanceof AllColumns)
			{
				fullFinalSchema.addAll(fullSchema);
				finalSchema.addAll(schema);
			}
			else if(selectItemslist.get(i) instanceof AllTableColumns)
			{

				AllTableColumns ac=((AllTableColumns)selectItemslist.get(i));
				String tableName=ac.getTable().getAlias().toLowerCase();
				if(tableName==null)
					tableName=ac.getTable().getName().toLowerCase();
				for(int j=0;j<fullFinalSchema.size();j++)
				{
					String str=fullFinalSchema.get(j).toLowerCase();
					String[] splits=str.split("\\.");
					if(splits[0].equals(tableName))
					{
						fullFinalSchema.add(str);
						finalSchema.add(splits[1]);
					}
				}
			}
		}
	}
	
	
	public List<Datum> processProjectListPerTuple(Datum[] tuple,String formedGroupByTuple,
			List<Datum> previousInMapTuple,Long currentCount)
	{
		Evaluator eval=new Evaluator(fullSchema,schema,tuple);
		List<Datum> outputDatumList=new ArrayList<Datum>();
		for(int i=0;i<selectItemslist.size();i++)
		{
			SelectExpressionItem st=(SelectExpressionItem) selectItemslist.get(i);
			Expression fnColExpr=st.getExpression();
			if(fnColExpr instanceof Function)
			{
				Function f=(Function)fnColExpr;		
				if(f.isAllColumns()==true)
				{
					if(!isFirstTuple)
					{
						outputDatumList.add(count(currentCount));
					}
					else{
						outputDatumList.add(count(0L));
					}
				}
				else
				{
					ExpressionList slist=f.getParameters();
					@SuppressWarnings("unchecked")
					List<Expression> exprList=slist.getExpressions();
					Expression expr=exprList.get(0);
					LeafValue leafValue=evaluateExpression(eval,expr);
					Datum t2=convertLeafToDatum(leafValue);
					if(!isFirstTuple)
					{
						Datum resultTuple=updateAggregateTuple(f.getName(),previousInMapTuple.get(i),t2,currentCount);
						outputDatumList.add(resultTuple);
					}
					else{
						if(!(f.getName().toLowerCase()).equals("count"))
						outputDatumList.add(t2);
						else
							outputDatumList.add(count(currentCount));
					}
				}
			}
			else
			{
				LeafValue leafValue=evaluateExpression(eval,fnColExpr);
				Datum colTuple=convertLeafToDatum(leafValue);
				outputDatumList.add(colTuple);
			}
		}
		return outputDatumList;
		
	}
	
	public void checkForHavingSchema()
	{
		if(havingExpr!=null)
		{
			for(SelectItem sItem:selectItemslist)
			{
				String temp=((SelectExpressionItem)sItem).getAlias();
				havingSchema.add(temp);
			}
		}
	}
	public void applyHavingFilter(Datum[] tuple)
	{
		Evaluator eval=new Evaluator();
		eval.setSchemaForHaving(havingSchema, tuple);
		if(evaluateHavingExpression(eval,havingExpr))
		{
			resultTuplesList.add(tuple);
		}
	}
	
		
	public List<Datum[]> getOutputList(){
		return resultTuplesList;
	}
	

 Other Methods Called from ReadOneTuple() Method 	
	private LeafValue evaluateExpression(Evaluator eval, Expression expr) {
		expr.accept(eval);
		LeafValue leafValue=eval.getOutput();
		return leafValue;
	}

	private boolean evaluateHavingExpression(Evaluator eval, Expression expr) {
		if(expr!=null){
		expr.accept(eval);
		Boolean bool=eval.getBool();
		return bool;
		}
		return true;
	}
	
	private Datum convertLeafToDatum(LeafValue leafValue) {
		Datum d;
		if(leafValue instanceof LongValue)
			d=new DatumLong(((LongValue) leafValue).getValue());
		else if(leafValue instanceof DoubleValue)
			d=new DatumDecimal(((DoubleValue) leafValue).getValue());
		else if(leafValue instanceof StringValue)
			d=new DatumString(((StringValue) leafValue).getValue());
		else if(leafValue instanceof DateValue)
			d=new DatumDate(((DateValue) leafValue).getValue());
		else
		{
			System.out.println("Sorry, Wrong Type Conversion During Projection!");
			return null;
		}
		return d;
	}
	
	
	private Datum updateAggregateTuple(String funcName,Datum resultColumn,
			Datum currentTupleColumn,Long tupleCount)
	{
	  funcName=funcName.toLowerCase();
	  switch(funcName)
	  {

		case "count":
		{
			resultColumn=count(tupleCount);
			break;
		}
		case "sum":
		{
			resultColumn=sum(resultColumn,currentTupleColumn);
			break;
		}
		case "avg":
		{
			resultColumn=avg(resultColumn,currentTupleColumn, tupleCount);
			break;
		}
		case "min":
		{
			resultColumn=min(resultColumn,currentTupleColumn);
			break;
		}
		case "max":
		{
			resultColumn=max(resultColumn,currentTupleColumn);
			break;
		}
		default:
		{
			System.out.println(funcName+":Not recognised Aggregate Operator!");
			break;
		}

	  }
		return resultColumn;
	}
	
	private Datum count(Long tupleCount) {
		return new DatumLong(tupleCount+1);
	}

	private Datum avg(Datum resultColumn, Datum currentTupleColumn, Long tupleCount) {
		if(resultColumn instanceof DatumLong)
		{
			Long val1=((DatumLong) resultColumn).getValue(); 
			Double val2=Double.parseDouble(currentTupleColumn.toString());
			Double val=(((val1*tupleCount)+val2)/(tupleCount+1));
			//val=Double.parseDouble(new DecimalFormat("##.####").format(val));
			resultColumn=new DatumDecimal(val);
			return resultColumn;
		}
		else if(resultColumn instanceof DatumDecimal)
		{
			Double val1=((DatumDecimal) resultColumn).getValue();
			Double val2=0.0;
			if(currentTupleColumn instanceof DatumDecimal)
			val2=Double.valueOf(((DatumDecimal) currentTupleColumn).getValue().toString());
			else if(currentTupleColumn instanceof DatumLong)
				val2=Double.valueOf(currentTupleColumn.toString());
			Double val=((val1*tupleCount)+val2)/(tupleCount+1);
			val=Double.parseDouble(new DecimalFormat("##.####").format(val));
			resultColumn=new DatumDecimal(val);
			return resultColumn;
		}
		else if(resultColumn instanceof DatumString)
		{
			System.out.println("Sorry! AVG() can't be calculated on String Datatype!");
		}
		else if(resultColumn instanceof DatumDate)
		{
			System.out.println("Sorry! AVG() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! AVG() can't be calculated on this Datatype!");
		}
		return null;
	}

	private Datum sum(Datum resultColumn, Datum currentTupleColumn) {
		if(resultColumn instanceof DatumLong)
		{
			Long val1=((DatumLong) resultColumn).getValue(); 
			Long val2=((DatumLong) currentTupleColumn).getValue();
			resultColumn=new DatumLong((val1+val2));
			return resultColumn;
		}
		else if(resultColumn instanceof DatumDecimal)
		{
			Double val1=((DatumDecimal) resultColumn).getValue(); 
			Double val2=((DatumDecimal) currentTupleColumn).getValue();
			resultColumn=new DatumDecimal((val1+val2));
			return resultColumn;
		}
		else if(resultColumn instanceof DatumString)
		{
			String val1=((DatumString) resultColumn).getValue(); 
			String val2=((DatumString) currentTupleColumn).getValue();
			resultColumn=new DatumLong((val1+val2));
			return resultColumn;
		}
		else if(resultColumn instanceof DatumDate)
		{
			System.out.println("Sorry! SUM() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! SUM() can't be calculated on this Datatype!");
		}
		return null;
	}


	
	

	private Datum max(Datum resultColumn, Datum currentTupleColumn) {
		if(resultColumn instanceof DatumLong)
		{
			Long val1=((DatumLong) resultColumn).getValue(); 
			Long val2=((DatumLong) currentTupleColumn).getValue();
			if((val2-val1)>=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(resultColumn instanceof DatumDecimal)
		{
			Double val1=((DatumDecimal) resultColumn).getValue(); 
			Double val2=((DatumDecimal) currentTupleColumn).getValue();
			if((val2-val1)>=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(resultColumn instanceof DatumString)
		{
			String val1=((DatumString) resultColumn).getValue(); 
			String val2=((DatumString) currentTupleColumn).getValue();
			if(val2.compareToIgnoreCase(val1)>=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(resultColumn instanceof DatumDate)
		{
			System.out.println("Sorry! MAX() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! MAX() can't be calculated on this Datatype!");
		}
		return null;
	}

	private Datum min(Datum resultColumn, Datum currentTupleColumn) {
		if(resultColumn instanceof DatumLong)
		{
			Long val1=((DatumLong) resultColumn).getValue(); 
			Long val2=((DatumLong) currentTupleColumn).getValue();
			if((val2-val1)<=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(resultColumn instanceof DatumDecimal)
		{
			Double val1=((DatumDecimal) resultColumn).getValue(); 
			Double val2=((DatumDecimal) currentTupleColumn).getValue();
			if((val2-val1)<=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(resultColumn instanceof DatumString)
		{
			String val1=((DatumString) resultColumn).getValue(); 
			String val2=((DatumString) currentTupleColumn).getValue();
			if(val2.compareToIgnoreCase(val1)<=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(resultColumn instanceof DatumDate)
		{
			System.out.println("Sorry! MIN() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! MIN() can't be calculated on this Datatype!");
		}
		return null;
	}

	
	@Override
	public void reset() {

	}

}
*/