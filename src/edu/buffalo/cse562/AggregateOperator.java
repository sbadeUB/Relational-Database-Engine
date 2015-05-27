package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.text.DecimalFormat;
import java.util.*;

public class AggregateOperator implements Operator {

	Operator input;
	List<SelectItem> selectItemslist;
	ArrayList<String> fullSchema;
	ArrayList<String> schema;
	ArrayList<String> fullFinalSchema=new ArrayList<String>();
	ArrayList<String> finalSchema=new ArrayList<String>();
	List<Tuple> aggregatesList=new ArrayList<Tuple>();
	List<Tuple> resultantTuple=new ArrayList<Tuple>();
	Long tupleCount;
	Tuple outtuple;
	public AggregateOperator(Operator input,List<SelectItem> selectItemslist, ArrayList<String> schema,ArrayList<String> schema2)
	{
		this.selectItemslist=selectItemslist;
		this.input=input;
		this.fullSchema=schema;
		this.schema=schema2;
		this.tupleCount=Long.parseLong("0");
	}
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple=null;
		tuple=input.readOneTuple();
		if(tuple==null)
		{
			 outtuple=executeAggregateList();
		}
		else
		{
			aggregatesList.add(tuple);
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
	
	//Should work it out
	public Tuple executeAggregateList()
	{
		StringBuilder sb=new StringBuilder();
		for(int i=0;i<selectItemslist.size();i++)
		{
			SelectExpressionItem st=(SelectExpressionItem) selectItemslist.get(i);
			Expression fnColExpr=st.getExpression();
			if(fnColExpr instanceof Function)
			{
				Function f=(Function)fnColExpr;	
				String resultTuple=calculateAggregates(f.getName(), aggregatesList, i);
				sb.append(resultTuple+"|");
			}
			else{
				sb.append(aggregatesList.get(0).gettupledata()[i]+"|");
			}
		}		
		String s=sb.toString();
		Tuple st= new Tuple(s,aggregatesList.get(0).gettupledatatypes());
		return st;
	}

	private String calculateAggregates(String funcName,List<Tuple> tuplesList,int atIndex)
	{
		String resultColumn = null;
		funcName=funcName.toLowerCase();
		switch(funcName)
		{

		case "count":
		{
			resultColumn=count(tuplesList);
			break;
		}
		case "sum":
		{
			resultColumn=sum(tuplesList,atIndex);
			break;
		}
		case "avg":
		{
			String resultCount=count(tuplesList);
			double count=Double.parseDouble(resultCount)+0.0;
			resultColumn=sum(tuplesList,atIndex);
			long aggsum=Long.parseLong(resultColumn);
			Double val= aggsum/count;
			val=Double.parseDouble(new DecimalFormat("##.####").format(val));
			resultColumn=val.toString();
			break;
		}
		case "min":
		{
			resultColumn=min(tuplesList,atIndex);
			break;
		}
		case "max":
		{
			resultColumn=max(tuplesList,atIndex);
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


	private String count(List<Tuple> tuplesList) {
		return String.valueOf(tuplesList.size());
	}


	private String sum(List<Tuple> tuplesList, int atIndex) {
		String in=tuplesList.get(0).gettupledatatypes()[atIndex];
		if(in.equalsIgnoreCase("int"))
		{
			Long sumresult = 0L;
			for(int i=0;i<tuplesList.size();i++){
				Long x=Long.parseLong((tuplesList.get(i).gettupledata()[atIndex]));
				sumresult=sumresult+x;
			}
		//	resulttuple=new String(sumresult);
			return sumresult.toString();
		}
		else if(in.equalsIgnoreCase("decimal"))
		{
			Double sumresult = 0.0;
			for(int i=0;i<tuplesList.size();i++){
				Double x=Double.parseDouble((tuplesList.get(i).gettupledata()[atIndex]));
				sumresult=sumresult+x;
			}
		//	resulttuple=new DatumDecimal(sumresult);
			return sumresult.toString();
		}
		else if(in.equalsIgnoreCase("String"))
		{
			StringBuffer sumresult = null;
			for(int i=0;i<tuplesList.size();i++){
				String x=(tuplesList.get(i).gettupledata()[atIndex]);
				sumresult=sumresult.append(x);
			}
			//resulttuple=new DatumString(sumresult.toString());
			return sumresult.toString();
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


	private String max(List<Tuple> tupleslist,int atIndex) {
		String in=tupleslist.get(0).gettupledatatypes()[atIndex];
		if(in.equalsIgnoreCase("long"))
		{
			Long max=Long.MIN_VALUE;
			for(int i=0;i<tupleslist.size();i++){
				if(Long.parseLong( tupleslist.get(i).gettupledata()[atIndex])>=max)
					max=Long.parseLong((tupleslist.get(i).gettupledata()[atIndex]));
			}
			return max.toString();
		}
		else if(in.equalsIgnoreCase("decimal"))
		{
			Double max=Double.MIN_VALUE;
			for(int i=0;i<tupleslist.size();i++){
				if(Double.parseDouble((tupleslist.get(i).gettupledata()[atIndex]))>=max)
					max=Double.parseDouble((tupleslist.get(i).gettupledata()[atIndex]));
			}
			return max.toString();
		}
		else if(in.equalsIgnoreCase("String"))
		{
			String max=tupleslist.get(0).gettupledata()[atIndex].toString();
			for(int i=1;i<tupleslist.size();i++){
				if((tupleslist.get(i).gettupledata()[atIndex]).compareToIgnoreCase(max)>=0)
					max=(tupleslist.get(i).gettupledata()[atIndex]);
			}
			return max.toString();
		}
		else if(in.equalsIgnoreCase("date"))
		{
			System.out.println("Sorry! MAX() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! MAX() can't be calculated on Date Datatype!");
		}
		return null;
	}



	private String min(List<Tuple> tupleslist,int atIndex) {
		String in=tupleslist.get(0).gettupledatatypes()[atIndex];
		if(in.equalsIgnoreCase("long"))
		{
			Long min=Long.MAX_VALUE;
			for(int i=0;i<tupleslist.size();i++){
				long localmin=Long.parseLong((tupleslist.get(i).gettupledata()[atIndex]));
				if(localmin<=min)
					min=localmin;
			}
			return min.toString();
		}
		else if(in.equalsIgnoreCase("decimal"))
		{
			Double min=Double.MAX_VALUE;
			for(int i=0;i<tupleslist.size();i++){
				Double localmin=Double.parseDouble((tupleslist.get(i).gettupledata()[atIndex]));
				if(localmin<=min)
					min=localmin;
			}
			return min.toString();
		}
		else if(in.equalsIgnoreCase("String"))
		{
			String min=tupleslist.get(0).gettupledata()[atIndex].toString();
			for(int i=1;i<tupleslist.size();i++){
				if((tupleslist.get(i).gettupledata()[atIndex]).compareToIgnoreCase(min)<=0)
					min=(tupleslist.get(i).gettupledata()[atIndex]);
			}
			return min.toString();
		}
		else if(in.equalsIgnoreCase("date"))
		{
			System.out.println("Sorry! MIN() can't be calculated on Date Datatype!");
		}
		else
		{
			System.out.println("Sorry! MIN() can't be calculated on this Datatype!");
		}
		return null;
	}


	public Tuple getOutput()
	{
		/*Datum[] result=new Datum[resultantTuple.size()];
		result= resultantTuple.toArray(result);*/
		return outtuple;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

}
