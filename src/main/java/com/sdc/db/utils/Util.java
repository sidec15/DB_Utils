/**
 * Utils.java
 */
package com.sdc.db.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.sdc.db.structures.DB_TYPE;
import com.sdc.file.structures.Table;
import com.sdc.file.exception.*;

/**
 * @author Simone De Cristofaro
 * @created 13/nov/2012
 */
public class Util {

	private static String TAB="\t";
	private static String separator=TAB+","+TAB;
	private static String delimiter="\"";
	private static final int maxQueryLogLength=50;
	
	
	public static void main(String args[]) throws Exception{
		long start=0;
		long stop=0;
		
//		String server=JOptionPane.showInputDialog("Insert the server where is installed MySQL");
//		String password=JOptionPane.showInputDialog("Insert the password");
//		String user=JOptionPane.showInputDialog("Insert an user");
//		String database=JOptionPane.showInputDialog("Insert a database");
//		String query=JOptionPane.showInputDialog("Insert a select query");
//		String [][]result= selectQuery(query, server, database, user, password,true);
		
		Table result= selectQuery("select * from snod",DB_TYPE.POSTGRES, "localhost",5432, "hyperpath_roma_32", "postgres", "postgres",true);
		System.out.println(result.toString());
//		
//		result= db.utils.Utils.selectQuery("select count(*) from strt",DB_TYPE.POSTGRES,"localhost",5432, "lazio", "postgres", "postgres",false);
//		long l=(Long) result.get("count", 0);
//		int i=(int) l;
//		System.out.println(i);
//		boolean resultSetInMemory=false;
//		
//		start=System.currentTimeMillis();
//		Table table= db.utils.Utils.selectQuery("select * from sp_sistema_its",DB_TYPE.POSTGRES,
//				"localhost",5432, "sp_nav2012_ita", "postgres", "postgres",true,resultSetInMemory);
//		System.out.println("ResulSet in Memory? " + resultSetInMemory);
//
//		stop=System.currentTimeMillis();
//		System.out.println("Count: " + table.getnRows());
//		System.out.println("time [ms]: " + (stop-start));
		
//		Table table= db.utils.Utils.selectQuery("select idno,tail,sped from strt",DB_TYPE.POSTGRES,
//				"localhost",5432, "lazio", "postgres", "postgres",true,resultSetInMemory);

		
//		DataBaseRreader dbr=new DataBaseRreader();
//		start=System.currentTimeMillis();
//		dbr.makeQuery("select * from people",DB_TYPE.POSTGRES,"localhost",5432, "test", "postgres", "postgres");
//		stop=System.currentTimeMillis();
//		String time=new SimpleDateFormat("hh:mm:ss").format(new Date(stop-start));
//		System.out.println("time: " + time);
//
//		StringBuilder sb=new StringBuilder();
//		
//		while(!dbr.endReader()) {
//			for(int i=1;i<=dbr.getnFields();i++)
//				sb.append(dbr.get(i)+",");
//			sb.delete(sb.length()-1, sb.length());
//			System.out.println(sb.toString());
//			sb.delete(0, sb.length());
//		}
		
//		Table table= db.utils.Utils.selectQuery("select  poin from snod limit 1",DB_TYPE.POSTGRES,
//		"localhost",5432, "lazio", "postgres", "postgres",true,true);
//		
//		PGgeometry po= (PGgeometry) table.getRandomElement(0);
//		Point p = (Point) po.getGeometry();
//		
//		System.out.println(p.x);
//		System.out.println(p.y);
		
//		//Test geometry
//		Table table= db.utils.Utils.selectQuery("select shap from strt limit 1",DB_TYPE.POSTGRES,
//		"localhost",5432, "lazio", "postgres", "postgres",true,true);
//
//		PGgeometry tmp=(PGgeometry) table.get(0, 0);
//		System.out.println(tmp);
//		
//		Table table2=new Table("test_poly",new String[] {"shap"});
//		table2.addRow(new Object[] {tmp});
//		
//		System.out.println(tmp.getGeometry());
//		System.out.println(tmp.getGeometry().getSrid());
//		System.out.println(tmp.getGeometry().getValue());
//		System.out.println(tmp.getGeoType());
//		System.out.println(tmp.getType());
//		System.out.println(tmp.getGeometry().getTypeString());
//		System.out.println(table2.getInsertQuery());
//		
//		db.utils.Utils.insertTableQuery(table2, DB_TYPE.POSTGRES, "localhost",5432, "lazio", "postgres", "postgres");
		
		
		//Test Date
//		Table table=new Table("test_date",new String[] {"cdat"});
//		table.addRow(new Object[] {Calendar.getInstance()});
//		System.out.println(table.toString());
//		Utils.insertTableQuery(table, DB_TYPE.POSTGRES, "localhost",5432, "hyperpath_roma_32", "postgres", "postgres");
		
		
		
		//Test empy table
		Table table=new Table("test_date",new String[] {"cdat"});
		Util.insertTableQuery(table, DB_TYPE.POSTGRES, "localhost",5432, "lazio", "postgres", "postgres");
		
		System.out.println("ok");
	}
	
	/**
	 * Make a select query, return the result set and export the table into a file
	 * @param query The atring of the query
	 * @param dbType Mysql or Postgres
	 * @param server
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 * @param countFirst If <code>true</code> count also the number of row of the result query
	 * @param targetTable The table to modify
	 * @param filePath Absolute file path of the export
	 * @param separator Filed separator in the exported file
	 * @param delimiter Filed delimiter in the exported file
	 * @param exportFormat 1 = txt, 2 = csv
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 * @throws TableException 
	 */
	public static Table selectQuery(String query,DB_TYPE dbType, String server,int port,
			String database,String user,String password,boolean countFirst,boolean resultSetInMemory,
			String filePath,String separator,String delimiter,int exportFormat) throws ClassNotFoundException, SQLException, IOException, TableException {
		
		System.out.println("Executing query: " + (query.length()>maxQueryLogLength?query.substring(0,maxQueryLogLength)+"...":query));
		try
		{
			if(dbType.equals(DB_TYPE.MYSQL))
				Class.forName("com.mysql.jdbc.Driver");
			else if(dbType.equals(DB_TYPE.POSTGRES))
				Class.forName("org.postgresql.Driver");
				
		} catch (ClassNotFoundException e) {
			 
			//System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			//e.printStackTrace();
			throw e;
		}
		List<Object[]> list_content=null;
		Connection conn=null;
		Statement stm=null;
		ResultSet rset=null;

		//Connection conn=DriverManager.getConnection( "jdbc:mysql://localhost/simone", "root", "zell88squall" );
		conn=DriverManager.getConnection("jdbc:"+ dbType.toString() +"://"+server+":"+port+"/"+database, user, password);

		stm=conn.createStatement();
		int righe=0;
		
		if(query.startsWith("select count")) countFirst=false;
		if(countFirst) {
			String countQuery=query.toLowerCase();
			int kk=countQuery.indexOf("from");
			countQuery="select count(*) " + countQuery.substring(kk,countQuery.length());
			rset=stm.executeQuery(countQuery);
			rset.next();
			righe=Integer.parseInt(""+ rset.getObject(1));
			stm=conn.createStatement();
		}
		if(!resultSetInMemory) {
			conn.setAutoCommit(false);
			stm.setFetchSize(1);
		}

		rset=stm.executeQuery(query);
		ResultSetMetaData rsmd=rset.getMetaData();//serve per ottenere info sul resultSet
		int colonne=rsmd.getColumnCount();
		//int righe=getNumRows(rset)+1;
		
		//riempio m
		//nomi campi
		String[] fields=new String[colonne];
		for(int j=1;j<colonne+1;j++)
			fields[j-1]=rsmd.getColumnLabel(j);
		
		//righe successive
		Object[] tmp_content=null;
		list_content=new ArrayList<Object[]>(righe);
		while(rset.next()) {
			tmp_content=new Object[colonne];
			for(int j=1;j<colonne+1;j++)
				tmp_content[j-1]=rset.getObject(j);
			list_content.add(tmp_content);
		}
		
		righe=list_content.size();
				
//		String[][] content=new String[righe][colonne];
//		for(int i=0;i<righe;i++)
//			content[i]=list_content.get(i);
			
		Table table=null;
		try
		{
			table = new Table("ResultSet",fields,(ArrayList<Object[]>) list_content,'#');
			if(filePath!=null && !filePath.equals("")) {
				//creo il file di testo della query
				com.sdc.file.utils.Util.createTable(table, filePath, "$", ":", ',','"');
			}
		} catch (IOException e) {
			throw e;
		} catch (TableException e) {
			throw e;
		}finally {
			if(rset!=null)
				rset.close();
			if(stm!=null)
				stm.close();
			if(conn!=null)
				conn.close();
		}
		return table;
	}
	
	/**
	 * Make a select query and return the result set
	 * @param query The atring of the query
	 * @param dbType Mysql or Postgres
	 * @param server
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 * @param countFirst If <code>true</code> count also the number of row of the result query
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 * @throws TableException 
	 */
	public static Table selectQuery(String query,DB_TYPE dbType, String server,int port,
			String database,String user,String password,boolean countFirst,boolean resultSetInMemory) throws ClassNotFoundException, SQLException, IOException, TableException {
		return selectQuery(query,dbType, server,port, database, user, password,countFirst,resultSetInMemory, "","","",1);
	}

	/**
	 * Make a select query and return the result set loading it in the memory
	 * @param query The atring of the query
	 * @param dbType Mysql or Postgres
	 * @param server
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 * @param countFirst If <code>true</code> count also the number of row of the result query
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 * @throws TableException 
	 */
	public static Table selectQuery(String query,DB_TYPE dbType, String server,int port,
			String database,String user,String password,boolean countFirst) throws ClassNotFoundException, SQLException, IOException, TableException {
		return selectQuery(query,dbType, server,port, database, user, password,countFirst,true, "","","",1);
	}

	
	/**
	 * Make a query that modify a specific table
	 * @param query The atring of the query
	 * @param dbType Mysql or Postgres
	 * @param server
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public static void modifyQuery(String query,DB_TYPE dbType, String server,int port,String database,String user,String password) throws SQLException, ClassNotFoundException, IOException  {
		
		System.out.println("Executing query: " + (query.length()>maxQueryLogLength?query.substring(0,maxQueryLogLength)+"...":query));
		try
		{
			if(dbType.equals(DB_TYPE.MYSQL))
				Class.forName("com.mysql.jdbc.Driver");
			else if(dbType.equals(DB_TYPE.POSTGRES))
				Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			 
			//System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			//e.printStackTrace();
			throw e;
		}
		Connection conn=null;
		PreparedStatement pstm=null;
		ResultSet rset=null;

		try {
			conn=DriverManager.getConnection("jdbc:"+dbType.toString()+"://"+server+":"+port+"/"+database, user, password);
			pstm=conn.prepareStatement(query);
		pstm.execute();			
		} catch (SQLException e) {
			throw e;
		}finally {
			if(rset!=null)
				rset.close();
			if(pstm!=null)
				pstm.close();
			if(conn!=null)
				conn.close();
		}
		
	}

	/**
	 * Make the query to insert the content of a local <code>Table</code> into the database
	 * @param table
	 * @param dbType
	 * @param server
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void insertTableQuery(Table table,DB_TYPE dbType, String server,int port,String database,String user,String password) throws ClassNotFoundException, SQLException, IOException {
		modifyQuery(table.getInsertQuery(), dbType, server, port, database, user, password);
	}
	
	/**
	 * Number of record of the resultset
	 * @param rset
	 * @return
	 * @throws SQLException
	 */
	private static int getNumRows(ResultSet rset) throws SQLException {
		rset.last();//sposto il cursore all'ultimo elemento della select
		int rows=rset.getRow();//prendo il numero dell'ultima riga
		rset.beforeFirst();//sposto il cursore prima del primo elemento della select
		return rows;
	}

	
}
