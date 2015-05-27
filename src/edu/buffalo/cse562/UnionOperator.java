package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

public class UnionOperator implements Operator {
	Operator input;
	List<Operator> operatorsforunion;
	ArrayList<String> mainSchema;
	ArrayList<String> schema;
	public UnionOperator(List<Operator> operatorsforunion,ArrayList<String> mainSchema,ArrayList<String> schema)
	{
		this.operatorsforunion=operatorsforunion;
		this.mainSchema=mainSchema;
		this.schema=schema;
	}

	@Override
	public Tuple readOneTuple() {
		
		for(int i=0;i<operatorsforunion.size();i++)
		{
			Tuple tuple=null;
			while((tuple=operatorsforunion.get(i).readOneTuple())!=null)
			{
				return tuple;
			}
		}
		return null;
	}

	@Override
	public void reset() {

	}

}
