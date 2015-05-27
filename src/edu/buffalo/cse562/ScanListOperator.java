package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ScanListOperator implements Operator {
	ArrayList<String> fullColumnNames;
	ArrayList<String> columnNames;
	List<Tuple> iteratorList=new ArrayList<Tuple>();
	Iterator<Tuple> itr;
	int listIndex=0;
	public ScanListOperator(List<Tuple> iteratorList,ArrayList<String> columnNames,ArrayList<String> fullColumnNames)
	{
		this.iteratorList=iteratorList;
		this.fullColumnNames=fullColumnNames;
		this.columnNames=columnNames;
		itr=iteratorList.iterator();
	}
	

	@Override
	public Tuple readOneTuple() {
		if(itr.hasNext()) return itr.next();
		else return null;
	}

	@Override
	public void reset() {
		itr=iteratorList.iterator();
	}

}
