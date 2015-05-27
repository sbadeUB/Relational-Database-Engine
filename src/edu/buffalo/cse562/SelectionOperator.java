package edu.buffalo.cse562;



import java.util.ArrayList;
import net.sf.jsqlparser.expression.Expression;

public class SelectionOperator implements Operator {
	Operator input;
	ArrayList<String> fullcolumnNames;
	ArrayList<String> columnNames;
	Expression condition;
	public SelectionOperator(Operator input,ArrayList<String> fullcolumnNames,ArrayList<String> columnNames,Expression condition) {
		this.input=input;
		this.fullcolumnNames=fullcolumnNames;
		this.columnNames=columnNames;
		this.condition=condition;
	}
	
	
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple=null;
		do{
			tuple=input.readOneTuple();
			if(tuple==null) { return null; }
			Evaluator eval=new Evaluator(fullcolumnNames,columnNames,tuple);
			condition.accept(eval);
			
			if(!eval.getBool())
			{
				tuple=null;
			}
			
		}while(tuple==null);
		
		return tuple;
	}

	@Override
	public void reset() {
		input.reset();
	}

}
