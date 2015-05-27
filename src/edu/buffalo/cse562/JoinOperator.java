/*package edu.buffalo.cse562;


import net.sf.jsqlparser.expression.Expression;
import java.util.ArrayList;



public class JoinOperator implements Operator{
	ArrayList<String> mainSchema;
	ArrayList<String> schema;
	Expression condition;
	Operator left;
	Operator  right;
	Datum[] lefttuple=null;
	Datum[] righttuple=null;
	public JoinOperator(ArrayList<String> mainSchema,ArrayList<String> schema,Operator left,Operator  right,Expression condition) {
		this.mainSchema=mainSchema;
		this.schema=schema;
		this.condition=condition;
		this.left=left;
		this.right=right;
	}

	@Override
	public Datum[] readOneTuple() {
		ArrayList<Datum> tuple = new ArrayList<Datum>();
		if(lefttuple==null)
		lefttuple=left.readOneTuple();
		do{
		do{
			righttuple=right.readOneTuple();
			if(righttuple== null)
			{
				lefttuple=left.readOneTuple();
				if(lefttuple==null)
				{
					return null;
				}
				right.reset();
				righttuple=right.readOneTuple();
			}
			for(int i=0;i<lefttuple.length;i++)
			{
				tuple.add(lefttuple[i]);
			}
			for(int i=0;i<righttuple.length;i++)
			{
				tuple.add(righttuple[i]);
			}
			if(tuple==null) { return null; }
			Datum[] tuples= new Datum[tuple.size()];
			tuple.toArray(tuples);
			if(condition!=null)
			{
				Evaluator eval=new Evaluator(mainSchema,schema,tuples);
				condition.accept(eval);
				if(!eval.getBool())
				{
					tuple.clear();;
				}
			}
			if(tuple.size()>0)
			{
				Datum[] tuples1= new Datum[tuple.size()];
				tuple.toArray(tuples1);
				return tuples1;
			}
		}while(tuple.size()==0);
		}while(lefttuple!=null);
		Datum[] tuples= new Datum[tuple.size()];
		tuple.toArray(tuples);
		return tuples;
	}

	@Override
	public void reset() {
	}

}
*/