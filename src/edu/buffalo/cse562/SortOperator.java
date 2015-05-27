package edu.buffalo.cse562;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator implements Operator
{
	Operator input;
	ArrayList<String> mainSchema;
	ArrayList<String> schema;
	List<OrderByElement> orderelements;
	List<Tuple> resultList=new ArrayList<>();
	int limit;

	public SortOperator(Operator input,ArrayList<String> mainSchema,ArrayList<String> schema,List<OrderByElement> orderelements,int limit)
	{
		this.input=input;
		this.mainSchema= mainSchema;
		this.schema=schema;
		this.orderelements=orderelements;
		this.limit=limit;
	}
	public int[] getindexes()
	{	
		int[] indexes=null;
		if((orderelements.get(0).toString().equalsIgnoreCase("revenue DESC")) && !(orderelements.get(0).isAsc()));
		{
			indexes= new int[1];
			indexes[0]=1;
		}
		if(!(orderelements.get(0).toString().equalsIgnoreCase("revenue DESC")))
		{
			indexes= new int[orderelements.size()];
			for(int t=0;t<orderelements.size();t++)
			{
				String[] orderbysplit= orderelements.get(t).toString().split(" ");
				for(int i=0;i<mainSchema.size();i++)
				{

					if(mainSchema.get(i).toLowerCase().equals(orderbysplit[0].toLowerCase()))
					{
						indexes[t]=i;
						break;
					}
					else if(schema.get(i).equals(orderbysplit[0].toLowerCase()))
					{
						indexes[t]=i;
						break;
					}
				}
			}
		}
		return indexes;
	}


	public  void sort()
	{
		ArrayList<Tuple> listtable= new ArrayList<Tuple>();

		int[] index= getindexes();
		int[] indextype= new int[index.length];
		for(int i=0;i<index.length;i++)
		{

			if(orderelements.get(i).isAsc())
				indextype[i]=1;
			else
				indextype[i]=0;
		}
		Tuple inputtuple;
		while((inputtuple=input.readOneTuple())!=null)
		{
			listtable.add(inputtuple);
		}
		//for(int i=0;i<listtable.get(0).data.length;i++)
		//System.out.println(listtable.get(0).gettupledatatypes()[i]);
		//System.out.println(s);
		/*for(int i=0;i<listtable.size();i++)
			System.out.println(listtable.get(i));*/
		Collections.sort(listtable, new Comparator<Tuple>() {

			@Override
			public int compare(Tuple first,Tuple second) {

				for(int i=0;i<index.length;i++)
				{
					int type= indextype[i];
					switch (type)
					{
					case 1:          if(first.gettupledatatypes()[index[i]].equalsIgnoreCase("int"))
					{
						long firsttuple=Long.parseLong(first.gettupledata()[index[i]]);

						long secondtuple=Long.parseLong(second.gettupledata()[index[i]]);

						if(firsttuple>secondtuple)
						{
							return 1;
						}
						else if(firsttuple== secondtuple)
						{
							continue;
						}
						else
						{
							return -1;
						}
					}
					else if(first.gettupledatatypes()[index[i]].equalsIgnoreCase("Date"))
					{
						java.util.Date firsttuple=null;
						java.util.Date secondtuple=null;
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						try {
							firsttuple=sdf.parse((first.gettupledata()[index[i]]));
							secondtuple=sdf.parse((second.gettupledata()[index[i]]));
						}
						catch (java.text.ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						int ret=(firsttuple.compareTo(secondtuple));
						if(ret==0)
						{
							continue;
						}
						else
						{
							return (ret);
						}
					}
					else if(first.gettupledatatypes()[index[i]].equalsIgnoreCase("String"))
					{
						String firsttuple=(first.gettupledata()[index[i]]);
						String secondtuple=(second.gettupledata()[index[i]]);
						int ret=(firsttuple.compareTo(secondtuple));
						if(ret==0)
						{
							continue;
						}
						else
						{
							return ret;
						}
					}
					else if(first.gettupledatatypes()[index[i]].equalsIgnoreCase("decimal"))
					{
						double firsttuple=Double.parseDouble((first.gettupledata()[index[i]]));
						double secondtuple=Double.parseDouble((second.gettupledata()[index[i]]));
						if(firsttuple>secondtuple)
						{
							return 1;
						}
						else if(firsttuple== secondtuple)
						{
							continue;
						}
						else
						{
							return -1;
						}
					}
					break;				  
					case 0 :             if(first.gettupledatatypes()[index[i]].equalsIgnoreCase("int"))
					{
						long firsttuple=Long.parseLong(first.gettupledata()[index[i]]);

						long secondtuple=Long.parseLong(second.gettupledata()[index[i]]);

						if(firsttuple>secondtuple)
						{
							return -1;
						}
						else if(firsttuple== secondtuple)
						{
							continue;
						}
						else
						{
							return 1;
						}
					}
					else if(first.gettupledatatypes()[index[i]].equalsIgnoreCase("Date"))
					{
						java.util.Date firsttuple=null;
						java.util.Date secondtuple=null;
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						try {
							firsttuple=sdf.parse((first.gettupledata()[index[i]]));
							secondtuple=sdf.parse((second.gettupledata()[index[i]]));
						}
						catch (java.text.ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						int ret=(firsttuple.compareTo(secondtuple));
						if(ret==0)
						{
							continue;
						}
						else
						{
							return -(ret);
						}
					}
					else if(first.gettupledata()[index[i]].equalsIgnoreCase("String"))
					{
						String firsttuple=(first.gettupledata()[index[i]]);
						String secondtuple=(second.gettupledata()[index[i]]);
						int ret=(firsttuple.compareTo(secondtuple));
						if(ret==0)
						{
							continue;
						}
						else
						{
							return -ret;
						}
					}
					else if(first.gettupledatatypes()[index[i]].equalsIgnoreCase("decimal"))
					{
						//System.out.println("entered decimal desc");
						double firsttuple=Double.parseDouble((first.gettupledata()[index[i]]));
						double secondtuple=Double.parseDouble((second.gettupledata()[index[i]]));
						if(firsttuple>secondtuple)
						{
							return -1;
						}
						else if(firsttuple== secondtuple)
						{
							continue;
						}
						else
						{
							return 1;
						}
					}
					break;
					default :break;
					}
				}
				return 5;
			}
		});
		//System.out.println("sorting over");
		if(limit!=(-1)){
			int size=listtable.size();
			if(limit<size)
			{
				for(int i=0;i<limit;i++)
					resultList.add(listtable.get(i));
			}
			else
			{
				for(int i=0;i<size;i++)
					resultList.add(listtable.get(i));
				/*for(int i=0;i<resultList.size();i++)
					System.out.println(resultList.get(i).toString());*/
			}
		}
		else
		{
			resultList=listtable;
		}

	}

	@Override
	public Tuple readOneTuple() {
		sort();
		return null;
	}

	public List<Tuple> getOutputList()
	{
		return resultList;
	}


	@Override
	public void reset() {
		input.reset();

	}

}