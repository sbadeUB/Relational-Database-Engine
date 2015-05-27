package edu.buffalo.cse562;

public class Tuple {
	public String[] data;
	public String[] datatyypes;
	public Tuple(String data,String[] datatypes) {
		this.data=data.split("\\|");
		this.datatyypes=datatypes;
	}
	public Tuple(int len) {
		data=new String[len];
		datatyypes=new String[len];
	}
	public String[] gettupledatatypes(){
		return this.datatyypes;
	}
	public String[] gettupledata(){
		return this.data;
	}
	@Override
	public String toString()
	{
		StringBuilder sb=new StringBuilder();
		String out="";
		for(int i=0;i<this.data.length;i++)
		{
			sb.append(data[i]+"|");
		}
		out=sb.toString();
		return out;		
	}
}
