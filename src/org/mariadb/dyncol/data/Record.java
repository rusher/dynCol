package org.mariadb.dyncol.data;

import org.mariadb.dyncol.DynCol;

public class Record {
	//use a enumeration
	/*
	 	NULL= 0,
		INT = 1,
		UINT = 2,
		DOUBLE = 3,
		STRING = 4,
		DECIMAL = 5,
		DATETIME = 6,
		DATE = 7,
		TIME = 8,
		DYNCOL = 9
	*/
	public int record_type;
	public long long_value;
	public double double_value;
	public String str_value = "";
	public DynCol DynCol_value;
}
