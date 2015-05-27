package edu.buffalo.cse562;

import java.util.ArrayList;

public class OperatorSchemasContainer {
	Operator oper;
	ArrayList<String> fullSchema=new ArrayList<String>();
	ArrayList<String> schema=new ArrayList<String>();
	public OperatorSchemasContainer(Operator oper,ArrayList<String> fullSchema,ArrayList<String> schema) {
		this.oper=oper;
		this.fullSchema=fullSchema;
		this.schema=schema;
	}

}
