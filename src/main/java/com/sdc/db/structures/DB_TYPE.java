/**
 * DB_TYPE.java
 */
package com.sdc.db.structures;

/**
 * @author Simone De Cristofaro
 * @created 13/nov/2012
 */
public enum DB_TYPE {

	POSTGRES{
    	public String toString() {        
    		return "postgresql";
    	}
	},MYSQL{
    	public String toString() {        
    		return "mysql";
    	}
	};

	public static DB_TYPE fromString(String s) {
		if(s.equals(MYSQL.toString())) return MYSQL;
		else if(s.equals(POSTGRES.toString())) return POSTGRES;
		return null;
	}
	
}
