/**
 * DataBaseReader.java
 */
package com.sdc.db.structures;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * @author Simone De Cristofaro
 * @created 14/nov/2012
 */
public class DataBaseReader {

	//ATTRIBUTES
	private ResultSet rset;
	private HashMap<String, Integer> coll_fields;//key: field name, value: field index
	private int nFields;
	private Connection conn;
	private Statement stm;
	private int currentRow;
	private final int maxQueryLogLength=50;
	
	
	//CONSTRUCTORS
	
	//METHODS
	/**
	 * Make the query
	 * @param query
	 * @param dbType
	 * @param server
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 * @param resultSetInMemory if true the wall resultset is put in the memory
	 * @return
	 */
	public boolean makeQuery(String query,DB_TYPE dbType, String server,int port,
			String database,String user,String password,boolean resultSetInMemory) {
		
		System.out.println("Executing query: " + (query.length()>maxQueryLogLength?query.substring(0,maxQueryLogLength)+"...":query));
		currentRow=-1;
		try
		{
			if(dbType.equals(DB_TYPE.MYSQL))
				Class.forName("com.mysql.jdbc.Driver");
			else if(dbType.equals(DB_TYPE.POSTGRES))
				Class.forName("org.postgresql.Driver");
				
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		
		conn = null;
		stm = null;

		//Connection conn=DriverManager.getConnection( "jdbc:mysql://localhost/simone", "root", "zell88squall" );
		try {
			conn=DriverManager.getConnection("jdbc:"+ dbType.toString() +"://"+server+":"+port+"/"+database, user, password);
			stm=conn.createStatement();
			if(!resultSetInMemory) {
				conn.setAutoCommit(false);
				stm.setFetchSize(1);
			}
			rset=stm.executeQuery(query);
			ResultSetMetaData rsmd=rset.getMetaData();//serve per ottenere info sul resultSet
			nFields=rsmd.getColumnCount();
			coll_fields=new HashMap<String, Integer>();
			for(int j=1;j<nFields+1;j++)
				coll_fields.put(rsmd.getColumnLabel(j),j);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		
		return true;

	}
	
	/**
	 * Make the query loading the resultset in the memory
	 * @param query
	 * @param dbType
	 * @param server
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 * @return
	 */
	public boolean makeQuery(String query,DB_TYPE dbType, String server,int port,
			String database,String user,String password) {
		return makeQuery(query, dbType, server, port, database, user, password,true);
	}
	
	public boolean endReader() {
		try {
			if(rset.next()) {
				currentRow++;
				return false;
			}
			else
				return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return true;
		}
		
		
	}

	/**
	 * Return the value of the specified column (starting from 1) at the current row
	 * @param fieldsIndex
	 * @return
	 */
	public Object get(int fieldsIndex) {
		try {
			return rset.getObject(fieldsIndex);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Return the value of the specified column at the current row
	 * @param fieldName
	 * @return
	 */
	public Object get(String fieldName) {
		return get(coll_fields.get(fieldName));
	}

	public boolean close() {
		try {
			if(rset!=null)
				rset.close();
			if(stm!=null)
				stm.close();
			if(conn!=null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	
	/**
	 * Return the index of the specified field
	 * @param fieldName
	 * @return
	 */
	public int getFieldIndex(String fieldName){
		return coll_fields.get(fieldName);
	}
	
	/**
	 * @return the nFields
	 */
	public int getnFields() {
		return nFields;
	}

	/**
	 * @return the currentRow
	 */
	public int getCurrentRow() {
		return currentRow;
	}

	
}
