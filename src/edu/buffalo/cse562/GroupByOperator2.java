package edu.buffalo.cse562;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByOperator2 implements Operator {
	
	Operator input;
	List<SelectItem> selectItemslist;
	List<Column> groupByList;
	ArrayList<String> fullSchema;
	ArrayList<String> schema;
	HashMap<String,Long> groupByTupleCounts=new HashMap<String,Long>();
	HashMap<String,Tuple> masterAggregateMap=new HashMap<String, Tuple>();
	List<Tuple> resultTuplesList=new ArrayList<Tuple>();
	boolean isFirstTuple;
	Expression havingExpr;
	ArrayList<String> fullFinalSchema=new ArrayList<String>();
	ArrayList<String> finalSchema=new ArrayList<String>();
	String[] inputdatatypes = null;
	int[] groupByIndexes;
	
	public GroupByOperator2(Operator oper,List<SelectItem> selectItemList,
							List<Column> groupByList,ArrayList<String> schema,ArrayList<String> schema2,Expression havingExpr)	{
		this.input=oper;
		this.selectItemslist=selectItemList;
		this.groupByList=groupByList;
		groupByIndexes = new int[groupByList.size()];
		this.fullSchema=schema;
		this.schema=schema2;
		this.isFirstTuple=true;
		this.havingExpr=havingExpr;
	}

	@Override
	public Tuple readOneTuple() {
		Tuple tuple=null;
		tuple=input.readOneTuple();
		if(tuple==null){ populateResultsList(); }
		else
		{
			StringBuffer formedGroupByTuple=new StringBuffer();
			Tuple previousInMapTuple=null;
			String[] tupleContents = tuple.gettupledata();
			for(int i=0;i<groupByIndexes.length;i++) formedGroupByTuple.append((tupleContents[groupByIndexes[i]])+":");
			String FormedGroupBytuple=formedGroupByTuple.toString();
			
			if(isFirstTuple) {
				formedGroupByTuple=  new StringBuffer();
				for(int i=0;i<groupByList.size();i++)
				{
					Column targetColumn=groupByList.get(i);
					int colID = -1;
					colID=fullSchema.indexOf(targetColumn.getWholeColumnName().toLowerCase());
					if(!targetColumn.toString().contains("."))
						colID=schema.indexOf(targetColumn.toString().toLowerCase());
					if(colID!=-1)
					{
						groupByIndexes[i] = colID;
						formedGroupByTuple.append((tupleContents[groupByIndexes[i]])+":");
					}
					else System.out.println("Sorry!,Wrong GroupByColumn found in GroupBy Clause!");
				}
				FormedGroupBytuple=formedGroupByTuple.toString();
				Tuple outputTuple=processProjectListPerTuple(tuple,null,0L);
				masterAggregateMap.put(FormedGroupBytuple, outputTuple);
				groupByTupleCounts.put(FormedGroupBytuple, 1L);
				inputdatatypes = tuple.gettupledatatypes();
				
				isFirstTuple=false;
				
			}
			else{
				if(masterAggregateMap.containsKey(FormedGroupBytuple))
				{
					previousInMapTuple = masterAggregateMap.get(FormedGroupBytuple);
					Long currentCount = groupByTupleCounts.get(FormedGroupBytuple);
					Tuple outputTuple = processProjectListPerTuple(tuple,previousInMapTuple,currentCount);
					masterAggregateMap.put(FormedGroupBytuple, outputTuple);
					groupByTupleCounts.put(FormedGroupBytuple, currentCount+1);
					
				}
				else
				{
					isFirstTuple=true;
					Tuple outputTuple=processProjectListPerTuple(tuple,null,0L);
					masterAggregateMap.put(FormedGroupBytuple, outputTuple);
					groupByTupleCounts.put(FormedGroupBytuple, 1L);
					isFirstTuple=false;
				}
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
	
	
	public Tuple processProjectListPerTuple(Tuple tuple,Tuple previousInMapTuple,Long currentCount)
	{
		StringBuilder sb=new StringBuilder();
		String[] previousTupleContents = null;
		if(previousInMapTuple!=null)
			previousTupleContents=previousInMapTuple.gettupledata();
		    String[] tupleContents = tuple.gettupledata();
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
						sb.append(count(currentCount).toString());
						sb.append("|");
					}
					else{
						sb.append(count(0L).toString());
						sb.append("|");
					}
				}
				else
				{
					if(!isFirstTuple)
					{
						String resultTupleContent=updateAggregateTuple(f.getName(),previousTupleContents[i],tupleContents[i],i,currentCount);
						sb.append(resultTupleContent);
						sb.append("|");
					}
					else{
						if(!(f.getName().toLowerCase()).equals("count")){
							sb.append(tuple.gettupledata()[i]);
							sb.append("|");
						}
						else{
							sb.append(count(currentCount));
							sb.append("|");
						}
					}
				}
			}
			else{
				sb.append(tuple.gettupledata()[i]);
				sb.append("|");
			}
		}
		String s = sb.toString();
		Tuple tp = new Tuple(s,inputdatatypes);
		return tp;
		
	}
	
	public void populateResultsList() {
		
		if(havingExpr!=null)
		{
			ArrayList<String> havingSchema=new ArrayList<String>();
			for(SelectItem sItem:selectItemslist)
			{
				String temp=((SelectExpressionItem)sItem).getAlias();
				havingSchema.add(temp);
			}
			for(Map.Entry<String,Tuple> e:masterAggregateMap.entrySet())
			{
				Tuple value=e.getValue();
				Evaluator eval=new Evaluator();
				eval.setSchemaForHaving(havingSchema, value);
				if(evaluateHavingExpression(eval,havingExpr))
				{
					resultTuplesList.add(value);
				}
			}
		}
		else
		{
			for(Map.Entry<String,Tuple> e:masterAggregateMap.entrySet())
			{
				Tuple value=e.getValue();
				resultTuplesList.add(value);
			}
		}
		
	}
	
	public List<Tuple> getOutputList(){
		return resultTuplesList;
	}
	

private boolean evaluateHavingExpression(Evaluator eval, Expression expr) {
		expr.accept(eval);
		Boolean bool=eval.getBool();
		return bool;
	}
	
	private String updateAggregateTuple(String funcName,String resultColumn,
			String currentTupleColumn,int atIndex,Long tupleCount)
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
			resultColumn=sum(resultColumn,currentTupleColumn,atIndex);
			break;
		}
		case "avg":
		{
			resultColumn=avg(resultColumn,currentTupleColumn,tupleCount,atIndex);
			break;
		}
		case "min":
		{
			resultColumn=min(resultColumn,currentTupleColumn,atIndex);
			break;
		}
		case "max":
		{
			resultColumn=max(resultColumn,currentTupleColumn,atIndex);
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
	
	private String count(Long tupleCount) {
		return String.valueOf(tupleCount+1);
	}

	private String avg(String resultColumn, String currentTupleColumn, Long tupleCount,int atIndex) {
		String in=inputdatatypes[atIndex].toLowerCase();
		if(in.equals("int") || in.equals("decimal"))
		{
			Double val1 = Double.parseDouble(resultColumn);
			Double val2=Double.parseDouble(currentTupleColumn.toString());
			Double val=(((val1*tupleCount)+val2)/(tupleCount+1));
			val=Double.parseDouble(new DecimalFormat("##.####").format(val));
			return val.toString();
		}		
		else if(in.equals("String"))
		{
			System.out.println("Sorry! AVG() can't be calculated on String Datatype!");
		}
		else if(in.equals("date"))
		{
			System.out.println("Sorry! AVG() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! AVG() can't be calculated on this Datatype!");
		}
		return null;
	}

	private String sum(String resultColumn, String currentTupleColumn, int atIndex) {
		String in=inputdatatypes[atIndex].toLowerCase();
		if(in.equals("int"))
		{
			Long val1=Long.parseLong(resultColumn); 
			Long val2=Long.parseLong(currentTupleColumn);			
			Long val=val1+val2;
			return val.toString();
		}
		else if(in.equals("decimal"))
		{
			Double val1=Double.parseDouble(resultColumn); 
			Double val2=Double.parseDouble(currentTupleColumn);
			Double val=val1+val2;
			return val.toString();
		}
		else if(in.equals("string"))
		{
			return resultColumn+currentTupleColumn;
		}
		else if(in.equalsIgnoreCase("date"))
		{
			System.out.println("Sorry! SUM() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! SUM() can't be calculated on this Datatype!");
		}
		return null;
	}


	
	

	private String max(String resultColumn, String currentTupleColumn,int atIndex) {
		String in=inputdatatypes[atIndex].toLowerCase();
		if(in.equals("int"))
		{
			Long val1=Long.parseLong(resultColumn); 
			Long val2=Long.parseLong(currentTupleColumn);
			if((val2-val1)>=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(in.equals("decimal"))
		{
			Double val1=Double.parseDouble(resultColumn); 
			Double val2=Double.parseDouble(currentTupleColumn);
			if((val2-val1)>=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(in.equals("string"))
		{
			if(currentTupleColumn.compareToIgnoreCase(resultColumn)>=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(in.equals("date"))
		{
			System.out.println("Sorry! MAX() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! MAX() can't be calculated on this Datatype!");
		}
		return null;
	}

	private String min(String resultColumn, String currentTupleColumn,int atIndex) {
		String in=inputdatatypes[atIndex].toLowerCase();
		if(in.equals("int"))
		{
			Long val1=Long.parseLong(resultColumn); 
			Long val2=Long.parseLong(currentTupleColumn);
			if((val2-val1)<=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(in.equals("decimal"))
		{
			Double val1=Double.parseDouble(resultColumn); 
			Double val2=Double.parseDouble(currentTupleColumn);
			if((val2-val1)<=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(in.equals("string"))
		{
			if(currentTupleColumn.compareToIgnoreCase(resultColumn)<=0) return currentTupleColumn;
			else return resultColumn;
		}
		else if(in.equals("date"))
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
