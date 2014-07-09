/**
 * DB_parameter.java
 */
package com.sdc.db.structures;

import com.sdc.db.structures.DB_TYPE;

/**
 * @author Simone De Cristofaro
 * @created 13/nov/2012
 */
public class DataBase {

	//ATTRIBUTEs
	private DB_TYPE dbType;
	private String server;
	private int port;
	private String user;
	private String password;
	private String database;
	
	
	//CONSTRUCTORS
	/**
	 * @param dbType
	 * @param server
	 * @param port
	 * @param user
	 * @param password
	 * @param database
	 */
	public DataBase(DB_TYPE dbType, String server, int port, String user,String password, String database) {
		super();
		setAllFields(dbType, server, port, user, password, database);
	}
	
	public DataBase(String s) {
		String[] db=s.split(":");
		DB_TYPE dbType=DB_TYPE.fromString(db[0]);
		String[] parameter=db[1].split(";");
		String server=parameter[0].split("=")[1];
		int port=Integer.parseInt(parameter[1].split("=")[1]);
		String user=parameter[2].split("=")[1];
		String password=parameter[3].split("=")[1];
		String database=parameter[4].split("=")[1];
		setAllFields(dbType, server, port, user, password, database);
	}
	
	
	//METHODS
	private void setAllFields(DB_TYPE dbType, String server, int port, String user,String password, String database) {
		this.dbType = dbType;
		this.server = server;
		this.port = port;
		this.user = user;
		this.password = password;
		this.database = database;
	}

	/**
	 * @return the dbType
	 */
	public DB_TYPE getDbType() {
		return dbType;
	}

	/**
	 * @param dbType the dbType to set
	 */
	public void setDbType(DB_TYPE dbType) {
		this.dbType = dbType;
	}

	/**
	 * @return the server
	 */
	public String getServer() {
		return server;
	}

	/**
	 * @param server the server to set
	 */
	public void setServer(String server) {
		this.server = server;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the database
	 */
	public String getDatabase() {
		return database;
	}

	/**
	 * @param database the database to set
	 */
	public void setDatabase(String database) {
		this.database = database;
	}



}
