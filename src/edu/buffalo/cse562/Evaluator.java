package edu.buffalo.cse562;


import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Evaluator  extends Eval implements ExpressionVisitor {

	ArrayList<String> fullcolumnNames;
	ArrayList<String> columnNames;
	String[] tupledata=null;
	String[] tupledatatypes=null;
	LeafValue leafValue;
	boolean isHavingSet=false;

	public Evaluator(ArrayList<String> fullColumnNames,ArrayList<String> ColumnNames,Tuple tuple) {
		this.fullcolumnNames=fullColumnNames;
		this.columnNames=ColumnNames;
		this.tupledata=tuple.gettupledata();
		this.tupledatatypes=tuple.gettupledatatypes();
	}
	public Evaluator(ArrayList<String> fullColumnNames,Tuple tuple) {
		this.fullcolumnNames=fullColumnNames;
		this.tupledata=tuple.gettupledata();
		this.tupledatatypes=tuple.gettupledatatypes();
	}
	public Evaluator() {
		isHavingSet=true;
	}
	public void setSchemaForHaving(ArrayList<String> havingSchema,Tuple tuple)
	{
		this.columnNames=havingSchema;
		this.tupledata=tuple.gettupledata();
		this.tupledatatypes=tuple.gettupledatatypes();
	}
	@Override
	public LeafValue eval(Column x){
		int colID=fullcolumnNames.indexOf(x.getWholeColumnName().toLowerCase());
		if(!x.toString().contains("."))
			colID=columnNames.indexOf(x.toString().toLowerCase());
		String datatype=this.tupledatatypes[colID];
		String data=this.tupledata[colID];
		if(datatype.equals("int") ){			
			LongValue lf=new LongValue(data);
			return lf;
		}
		else if(datatype.equals("decimal")){
			DoubleValue df=new DoubleValue(data);
			return df;
		}
		else if(datatype.equals("string")){
			StringValue sf=new StringValue("'"+data+"'");
			return sf;
		}
		else if(datatype.equals("date")){
			DateValue dtf=new DateValue(" "+data+" ");
			return dtf;
		}
		else{
			return null;
		}
	}
	@Override
	public void visit(NullValue a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Function a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(InverseExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(JdbcParameter a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(DoubleValue a) {
		leafValue=this.eval(a);
	}

	@Override
	public void visit(LongValue a) {
		leafValue=this.eval(a);
	}

	@Override
	public void visit(DateValue a) {
		leafValue=this.eval(a);
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(StringValue a) {
		leafValue=this.eval(a);
	}

	@Override
	public void visit(Addition a) {

		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@Override
	public void visit(Division a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Multiplication a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Subtraction a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(AndExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(OrExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Between a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(EqualsTo a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(GreaterThan a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(GreaterThanEquals a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(InExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(IsNullExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(LikeExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(MinorThan a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(MinorThanEquals a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(NotEqualsTo a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		leafValue=eval(arg0);

	}

	@Override
	public void visit(SubSelect a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(CaseExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(WhenClause a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(ExistsExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(AllComparisonExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(AnyComparisonExpression a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Concat a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Matches a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(BitwiseAnd a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(BitwiseOr a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(BitwiseXor a) {
		try {
			leafValue=this.eval(a);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public boolean getBool() {
		String s=leafValue.toString();
		if(s.equalsIgnoreCase("false"))
			return false;
		else return true;
	}
	public LeafValue getOutput(){
		return leafValue;
	}
	

}
