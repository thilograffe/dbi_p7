package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Start {
	Connection con;
	final int scaleAcc = 100000;
	final int scaleTel = 10;
	final int batchSize = 10000;
	
	static final int mode = 	1;
	static final int anzahl = 	50;
	
	public static void main(String[] args) throws SQLException {
		Start start = new Start();
		start.connect();
		if(mode==0) {
			start.dropTables();
			start.createTables();
		}
		else if(mode==1) {
			start.createSQLPreparedStatement(anzahl);
		}
		start.disconnect();
	}
	
	public void connect() {
		try {
			con = DriverManager.getConnection("jdbc:postgresql:postgres", "postgres", "datenbank");
			con.setAutoCommit(false);
			System.out.println("Verbunden!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void disconnect() {
		try {
			con.close();
			System.out.println("Disconnected!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void createTables() {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("create table branches\r\n" + 
					"( branchid int not null,\r\n" + 
					" branchname char(20) not null,\r\n" + 
					" balance int not null,\r\n" + 
					" address char(72) not null,\r\n" + 
					" primary key (branchid) );\r\n" + 
					"create table accounts\r\n" + 
					"( accid int not null,\r\n" + 
					" name char(20) not null,\r\n" + 
					" balance int not null,\r\n" + 
					"branchid int not null,\r\n" + 
					"address char(68) not null,\r\n" + 
					"primary key (accid),\r\n" + 
					"foreign key (branchid) references branches ); " +
					"create table tellers\r\n" + 
					"( tellerid int not null,\r\n" + 
					" tellername char(20) not null,\r\n" + 
					" balance int not null,\r\n" + 
					" branchid int not null,\r\n" + 
					" address char(68) not null,\r\n" + 
					" primary key (tellerid),\r\n" + 
					" foreign key (branchid) references branches );\r\n" + 
					"create table history\r\n" + 
					"( accid int not null,\r\n" + 
					" tellerid int not null,\r\n" + 
					" delta int not null,\r\n" + 
					" branchid int not null,\r\n" + 
					" accbalance int not null,\r\n" + 
					" cmmnt char(30) not null,\r\n" + 
					" foreign key (accid) references accounts,\r\n" + 
					" foreign key (tellerid) references tellers,\r\n" + 
					" foreign key (branchid) references branches ); ");
			con.commit();
			System.out.println("tables erstellt!");
			stmt.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void dropTables() {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("drop table if exists history, accounts, branches, tellers;");
			con.commit();
			System.out.println("gedroppped!");
			stmt.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void createSQLPreparedStatement(int n) {
		long start = System.currentTimeMillis();
		try {
			PreparedStatement stmt = con.prepareStatement(
				"insert into branches values (?, ?, ?, ?)");

			for(int i = 1; i <= n; i++) {
				stmt.setInt(1, i);
				stmt.setString(2, "AutomobileAutomobile");
				stmt.setInt(3, 0);
				stmt.setString(4, "jlollduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn");
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.close();
			stmt = con.prepareStatement(
					"insert into accounts values (?, ?, ?, ?, ?)");
			for(int j=0;j<batchSize;j++) {
				
			}
			
			for(int i = 0; i < (scaleAcc*n)/batchSize; i++) {
				for(int j=0;j<batchSize;j++) {
					stmt.setInt(1, i*batchSize+j+1);
					stmt.setString(2, "AutomobileAutomobile");
					stmt.setInt(3, 0);
					stmt.setInt(4, (int)(Math.random()*n)+1);
					stmt.setString(5, "lduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn");
					stmt.addBatch();
				}
				stmt.executeBatch();
			}
			stmt.close();
			stmt = con.prepareStatement(
					"insert into tellers values (?, ?, ?, ?, ?)");
			
			for(int i = 1; i <= scaleTel *n; i++) {
				stmt.setInt(1, i);
				stmt.setString(2, "AutomobileAutomobile");
				stmt.setInt(3, 0);
				stmt.setInt(4, (int)(Math.random()*n)+1);
				stmt.setString(5, "lduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn");
				stmt.addBatch();
			}
			stmt.executeBatch();
			con.commit();
			System.out.println(System.currentTimeMillis() - start);
			
			
			stmt.close();
			
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
