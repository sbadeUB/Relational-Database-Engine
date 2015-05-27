package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;

import net.sf.jsqlparser.expression.Expression;

public class InMemoryHybridHashJoin implements Operator{

	Operator left;
	Operator right;;
	Expression condition;
	ArrayList<String> schema;
	ArrayList<String> mainschema;
	ArrayList<Tuple> joinedtable= new ArrayList<>();
	ListIterator<Tuple> itr=null;
	HashMap<String,ArrayList<Tuple>> hybridtable= new HashMap<>();
	int[] indexes= new int[2];
	String[] joinedtabledatatypes;
	boolean isfirst=false;
	Tuple righttuple=null;

	public InMemoryHybridHashJoin(ArrayList<String> mainschema,ArrayList<String> schema,Operator left,Operator right,Expression condition)
	{
		this.schema=schema;
		this.mainschema=mainschema;
		this.left=left;
		this.right=right;
		this.condition=condition;
		getindexes();
		isfirst=true;
		join();
	}
	public int[] getindexes()
	{
		String[] tablesdotcoloumns=condition.toString().split("=");
		Tuple lefttuple= left.readOneTuple();
		int llen=lefttuple.gettupledata().length;
		String[] leftcoldataytypes=lefttuple.gettupledatatypes();
		left.reset();
		Tuple righttuple= right.readOneTuple();
		String[] rightcoldataytypes=righttuple.gettupledatatypes();
		right.reset();
		for(int i=0;i<mainschema.size();i++)
		{
			if(mainschema.get(i).toString().equals(tablesdotcoloumns[0].trim()))
			{
				indexes[0]=i;
			}
			if(mainschema.get(i).toString().equals(tablesdotcoloumns[1].trim()))
			{
				indexes[1]=i;
			}
		}
		if(indexes[0]>indexes[1])
		{
			int temp=indexes[0];
			indexes[0]=indexes[1];
			indexes[1]= temp;
		}
		combineColdatatypes(leftcoldataytypes,rightcoldataytypes);
		indexes[1]=indexes[1]-llen;
		return indexes;
	}

	private void combineColdatatypes(String[] leftcoldataytypes,
			String[] rightcoldataytypes) {
		this.joinedtabledatatypes=new String[leftcoldataytypes.length+rightcoldataytypes.length];
		System.arraycopy(leftcoldataytypes, 0, joinedtabledatatypes, 0, leftcoldataytypes.length);
		System.arraycopy(rightcoldataytypes, 0, joinedtabledatatypes, leftcoldataytypes.length, rightcoldataytypes.length);
	}
	
	public  Tuple concatenate (Tuple a, Tuple b) {
		
		int aLen = a.gettupledata().length;
		int bLen = b.gettupledata().length;
		Tuple c = new Tuple(aLen+bLen);
		System.arraycopy(a.gettupledata(), 0, c.data, 0, aLen);
		System.arraycopy(b.gettupledata(), 0, c.data, aLen, bLen);
		c.datatyypes=this.joinedtabledatatypes;
		return c;
	}

	public void join()
	{
		Tuple lefttuple;
		//long start=System.currentTimeMillis();
		while((lefttuple=left.readOneTuple())!=null)
		{			
			String joininginteger= lefttuple.gettupledata()[indexes[0]];
			if(hybridtable.containsKey(joininginteger))
			{
				ArrayList<Tuple> addtuples=hybridtable.get(joininginteger);
				addtuples.add(lefttuple);
			}
			else
			{
				ArrayList<Tuple> newlyadded=new ArrayList<>();
				newlyadded.add(lefttuple);
				hybridtable.put(joininginteger, newlyadded);
			}
		}
		//long end1=System.currentTimeMillis();
		//double lefttime=(end1-start)/1000;
		//System.out.println("Time taken to fill left hashmap:"+lefttime);
		//itr=joinedtable.iterator();
		/*long end2=System.currentTimeMillis();
		double righttime=(end2-end1)/1000;
		System.out.println("Time taken to fill joined table:"+righttime);*/
	}

	

	@Override
	public Tuple readOneTuple() {
		
		Tuple outtuple=null;
		do{
			
			if(joinedtable.size()==0){
				//long start=System.currentTimeMillis();
				righttuple=right.readOneTuple();
				//long end=System.currentTimeMillis();
				//System.out.println("Time to create 1 Nested set:"+(end-start));
				if(righttuple==null && joinedtable.size()==0) return null;
				String searchtuple=righttuple.gettupledata()[indexes[1]];
				
				if(hybridtable.containsKey(searchtuple)){					
					ArrayList<Tuple> lefttuples=hybridtable.get(searchtuple);
					combine(lefttuples,righttuple);
				}
				else righttuple=null;
			}
			this.itr=joinedtable.listIterator();
			if(itr.hasNext()){
				outtuple=itr.next();
				//if(outtuple!=null) return outtuple;
				if(!isfirst)
				itr.remove();
				else
					isfirst=false;
			}	
			else outtuple=null;
		}while(righttuple==null);
		
		return outtuple;
	}

	private void combine(ArrayList<Tuple> lefttuples, Tuple searchtuple) {
		for(int i=0;i<lefttuples.size();i++){
			Tuple joinedtuple=concatenate(lefttuples.get(i), searchtuple);
			joinedtable.add(joinedtuple);
		}
	}
	@Override
	public void reset() {
		if(itr.hasPrevious()) itr.previous();
		itr=joinedtable.listIterator();
	}



}