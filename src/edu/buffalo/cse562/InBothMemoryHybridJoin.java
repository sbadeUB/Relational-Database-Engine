/*package edu.buffalo.cse562;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;

public class InBothMemoryHybridJoin implements Operator{

	Operator left;
	Operator right;;
	Expression condition;
	ArrayList<String> schema;
	ArrayList<String> mainschema;
	ArrayList<Datum[]> joinedtable= new ArrayList<>();
	Iterator<Datum[]> itr=null;
	HashMap<String,ArrayList<Datum[]>> hybridtable= new HashMap<>();
	HashMap<String,ArrayList<Datum[]>> hybridsecondtable= new HashMap<>();
	int[] indexes= new int[2];

	public InBothMemoryHybridJoin(ArrayList<String> mainschema,ArrayList<String> schema,Operator left,Operator right,Expression condition)
	{
		this.schema=schema;
		this.mainschema=mainschema;
		this.left=left;
		this.right=right;
		this.condition=condition;
		getindexes();
		//getIndexes(condition);
		join();
		itr=joinedtable.iterator();
		//System.out.println("Done!");
	}
	public int[] getindexes()
	{
		String[] tablesdotcoloumns=condition.toString().split("=");
		Datum[] lefttuple= left.readOneTuple();
		if(lefttuple==null)
		{
			System.out.println(condition.toString());
		}
		left.reset();
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
		indexes[1]=indexes[1]-lefttuple.length;
		//System.out.println("indexleft:"+indexes[0]+"\n"+"indexright:"+indexes[1]);
		return indexes;
	}


	public  Datum[] concatenate (Datum[] a, Datum[] b) {
		int aLen = a.length;
		int bLen = b.length;
		Datum[] c = (Datum[]) Array.newInstance(a.getClass().getComponentType(), aLen+bLen);
		System.arraycopy(a, 0, c, 0, aLen);
		System.arraycopy(b, 0, c, aLen, bLen);
		return c;
	}

	
	
	public void join()
	{
		Datum[] lefttuple;
		long start=System.currentTimeMillis();
		while((lefttuple=left.readOneTuple())!=null)
		{
			Datum joininginteger= lefttuple[indexes[0]];
			if(hybridtable.containsKey(joininginteger.toString().trim()))
			{
				 ArrayList<Datum[]> addtuples=hybridtable.get(joininginteger.toString());
				 addtuples.add(lefttuple);
			}
			else
			{
				ArrayList<Datum[]> newlyadded=new ArrayList<>();
				newlyadded.add(lefttuple);
				hybridtable.put(joininginteger.toString(), newlyadded);
			}
		}
		long end=System.currentTimeMillis();
		System.out.println("Time to build left map:"+(end-start)/1000);
		Datum[] righttuple;
		while((righttuple=right.readOneTuple())!=null)
		{
			Datum joininginteger= righttuple[indexes[1]];
			if(hybridsecondtable.containsKey(joininginteger.toString()))
			{
				 ArrayList<Datum[]> addtuples=hybridsecondtable.get(joininginteger.toString());
				 addtuples.add(righttuple);
			}
			else
			{
				ArrayList<Datum[]> newlyadded=new ArrayList<>();
				newlyadded.add(righttuple);
				hybridsecondtable.put(joininginteger.toString(), newlyadded);
			}
		}
		long end1=System.currentTimeMillis();
		System.out.println("Time to build right map:"+(end1-end)/1000);
		for(String i:hybridsecondtable.keySet())
		{
			
			if(hybridtable.containsKey(i))
			{
				ArrayList<Datum[]> lefttuples=hybridtable.get(i);
				ArrayList<Datum[]> righttuples=hybridsecondtable.get(i);
				for(int i2=0;i2<lefttuples.size();i2++)
				{
					for(int j=0;j<righttuples.size();j++)
					{
						Datum[] joinedtuple=concatenate(lefttuples.get(i2), righttuples.get(j));
						joinedtable.add(joinedtuple);
						//System.out.println("good morning");
					}
				}
			}
		}
		long end2=System.currentTimeMillis();
		System.out.println("Time to build JOIN TABLE:"+(end2-end1)/1000);
		System.out.println("done");
	}

	@Override
	public Datum[] readOneTuple() {
		if(itr.hasNext()) return itr.next(); 
		else return null;
	}

	@Override
	public void reset() {
		this.itr=joinedtable.iterator();
	}



}*/