package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator implements Operator {

	Operator input;
	List<SelectItem> selectItemslist;
	ArrayList<String> fullSchema;
	ArrayList<String> schema;
	ArrayList<String> fullFinalSchema=new ArrayList<String>();
	ArrayList<String> finalSchema=new ArrayList<String>();
	String[] outcoldatatypes;

	public ProjectionOperator(Operator input,List<SelectItem> selectItemslist, ArrayList<String> schema,ArrayList<String> schema2)
	{
		this.selectItemslist=selectItemslist;
		this.input=input;
		this.fullSchema=schema;
		this.schema=schema2;
		outcoldatatypes=new String[selectItemslist.size()];
		getoutcoldatatypes();
	}

	public void getoutcoldatatypes()
	{
		Tuple tuple=this.input.readOneTuple();
		input.reset();
		Expression condition = null;
		ArrayList<String> coldatatypelist=new ArrayList<String>(selectItemslist.size());
		for(SelectItem s:selectItemslist)
		{
			Evaluator eval=new Evaluator(fullSchema,schema,tuple);
			if(s instanceof SelectExpressionItem)	
			{
				condition=((SelectExpressionItem) s).getExpression();
				if(condition instanceof Function){
					Function f=(Function)condition;
					if(!f.getName().startsWith("count"))
					{
						ExpressionList exprlist=f.getParameters();
						@SuppressWarnings("unchecked")
						List<Expression> listExpressions=exprlist.getExpressions();
						Expression expr=listExpressions.get(0);					
						expr.accept(eval);
						LeafValue output=eval.getOutput();
						if(output instanceof LongValue)
							coldatatypelist.add("int");
						else if(output instanceof DoubleValue)
							coldatatypelist.add("decimal");
						else if(output instanceof StringValue)
							coldatatypelist.add("string");
						else if(output instanceof DateValue)
							coldatatypelist.add("date");
						else{System.out.println("Sorry, Wrong Type Conversion while getting coldatatypes in Projection!");}
					}
					else{
						coldatatypelist.add("int");
					}
				}
				else{
					condition.accept(eval);
					LeafValue leafValue=eval.getOutput();
					if(leafValue instanceof LongValue)
						coldatatypelist.add("int");
					else if(leafValue instanceof DoubleValue)
						coldatatypelist.add("decimal");
					else if(leafValue instanceof StringValue)
						coldatatypelist.add("string");
					else if(leafValue instanceof DateValue)
						coldatatypelist.add("date");
					else{ System.out.println("Sorry, Wrong Type Conversion During Projection!"); }
				}
			}
			else if(s instanceof AllTableColumns)
			{
				//Should workitout
				String tablename=((AllTableColumns) s).getTable().getName().toLowerCase();
				for(int i=0;i<tuple.gettupledata().length;i++)
				{
					String sl=fullSchema.get(i).toLowerCase();
					String[] strs=sl.split("\\.");
					if(strs[0].equals(tablename)) coldatatypelist.add(tuple.gettupledatatypes()[i]);
				}
			}
		}
		outcoldatatypes=coldatatypelist.toArray(outcoldatatypes);
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



	@Override
	public Tuple readOneTuple() {
		Tuple tuple=null;		
		Expression condition = null;
		tuple=input.readOneTuple();
		if(tuple==null) { return null; }
		StringBuilder sb=new StringBuilder();
		for(SelectItem s:selectItemslist)
		{
			Evaluator eval=new Evaluator(fullSchema,schema,tuple);
			if(s instanceof SelectExpressionItem)	
			{
				condition=((SelectExpressionItem) s).getExpression();
				if(condition instanceof Function){
					Function f=(Function)condition;	
					if(!f.getName().startsWith("count"))
					{
						ExpressionList exprlist=f.getParameters();
						@SuppressWarnings("unchecked")
						List<Expression> listExpressions=exprlist.getExpressions();
						Expression expr=listExpressions.get(0);					
						expr.accept(eval);
						LeafValue output=eval.getOutput();
						String d = null;
						d=output.toString();
						sb.append(d+"|");
					}
					else{
						sb.append("1|");
					}
				}
				else{
					condition.accept(eval);
					LeafValue leafValue=eval.getOutput();
					String d=leafValue.toString();					
					sb.append(d+"|");
				}
			}
			else if(s instanceof AllTableColumns)
			{
				//should workit out
				String tablename=((AllTableColumns) s).getTable().getName().toLowerCase();
				for(int i=0;i<tuple.gettupledata().length;i++)
				{
					String sl=fullSchema.get(i).toLowerCase();
					String[] strs=sl.split("\\.");
					if(strs[0].equals(tablename)) sb.append(tuple.gettupledata()[i]+"|");
				}
			}
		}
		tuple=new Tuple(sb.toString(),this.outcoldatatypes);
		return tuple;
	}

	@Override
	public void reset() {
		input.reset();
	}

}
