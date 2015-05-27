package edu.buffalo.cse562;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public interface Datum {

	int hashcode=0;
	public String toString();
	public int getHashCode();
	@Override
	public boolean equals(Object j);
	public class DatumLong implements Datum,Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 0L;
		Long longval;
		public DatumLong(String val) {
			this.longval=Long.parseLong(val);
		}
		
		public DatumLong(long value) {
			this.longval=value;
		}

		@Override
		public String toString()
		{
			return this.longval.toString();
		}
		
		public Long getValue()
		{
			return this.longval;
		}

		@Override
		public int getHashCode() {
			// TODO Auto-generated method stub
			return this.longval.intValue();
		}
		
		@Override
		public boolean equals(Object j1){
			Datum j=(Datum) j1;
		    final DatumLong other = (DatumLong) j;
		    if (!this.longval.equals(other.longval)) {
		        return false;
		    }
		    return true;			
		}
		
	}
	public class DatumDecimal implements Datum,Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		Double decimalval;
		public DatumDecimal(String val) {
			this.decimalval=Double.parseDouble(val);
		}
		public DatumDecimal(double value) {
			this.decimalval=value;
		}
		@Override
		public String toString()
		{
			return this.decimalval.toString();
		}
		public Double getValue()
		{
			return this.decimalval;
		}
		@Override
		public int getHashCode() {
			// TODO Auto-generated method stub
			return this.decimalval.intValue();
		}
		@Override
		public boolean equals(Object j1){
			Datum j=(Datum) j1;
		    final DatumDecimal other = (DatumDecimal) j;
		    if (!this.decimalval.equals(other.decimalval)) {
		        return false;
		    }
		    return true;			
		}
	}
	public class DatumString implements Datum,Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 2L;
		String stringval;
		public DatumString(String val) {
			this.stringval=val;
		}
		public String getValue()
		{
			return this.stringval;
		}
		@Override
		public String toString()
		{
			return this.stringval.toString();
		}
		@Override
		public int getHashCode() {
			// TODO Auto-generated method stub
			return this.stringval.length();
		}
		@Override
		public boolean equals(Object j1){
			Datum j=(Datum) j1;
		    final DatumString other = (DatumString) j;
		    if (!this.stringval.equals(other.stringval)) {
		        return false;
		    }
		    return true;			
		}
		
	}
	public class DatumDate implements Datum,Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 3L;
		Date date;
		int year;
		int monthNum;
		int DayNum;
		String dateString;
		public DatumDate(String val)
		{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
		    try {
		    	this.date=sdf.parse(val);
				this.dateString = sdf.format(this.date);
				cal.setTime(this.date);
			    this.year = cal.get(Calendar.YEAR);
			    this.monthNum = cal.get(Calendar.MONTH)+1;
			    this.DayNum = cal.get(Calendar.DAY_OF_MONTH);
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public DatumDate(Date date) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			this.date=date;
			this.dateString = sdf.format(this.date);
			cal.setTime(this.date);
		    this.year = cal.get(Calendar.YEAR);
		    this.monthNum = cal.get(Calendar.MONTH)+1;
		    this.DayNum = cal.get(Calendar.DAY_OF_MONTH);
		}
		@Override
		public String toString()
		{
			return String.format("%04d-%02d-%02d", year, monthNum, DayNum);
		}
		public Date getValue()
		{
			return this.date;
		}
		@Override
		public int getHashCode() {
			// TODO Auto-generated method stub
			return this.dateString.length();
		}
		
		@Override
		public boolean equals(Object j1){
			Datum j=(Datum) j1;
		    final DatumDate other = (DatumDate) j;
		    if (!this.dateString.equals(other.dateString)) {
		        return false;
		    }
		    return true;			
		}
	}
}
