package jdbcPostgres;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class TryWithCopy {

	Connection con;
	
	public static void main(String[] args) {
		new TryWithCopy();
	}
	
	public TryWithCopy() {
		connect();
		dropTables();
		createTables();
		disableTriggers();
		int n = 10;
		try {
			long start = System.currentTimeMillis();
			createAccounts(n);
			createBranches(n);
			createTellers(n);
			
			CopyManager mgr = new CopyManager((BaseConnection)con);
			Reader in = new BufferedReader(new FileReader("csv/branches.csv"));
			mgr.copyIn("copy branches from stdin delimiter ',' CSV header",in);
			System.out.println(System.currentTimeMillis()-start);
			in = new BufferedReader(new FileReader("csv/accounts.csv"));
			mgr.copyIn("copy accounts from stdin delimiter ',' CSV header",in);
			System.out.println(System.currentTimeMillis()-start);
			in = new BufferedReader(new FileReader("csv/tellers.csv"));
			mgr.copyIn("copy tellers from stdin delimiter ',' CSV header",in);
			
			System.out.println(System.currentTimeMillis()-start);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("fertig");
	}
	
	private void disableTriggers() {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("ALTER TABLE accounts DISABLE TRIGGER ALL;\r\n" + 
								"ALTER TABLE branches DISABLE TRIGGER ALL;\r\n" + 
								"ALTER TABLE tellers DISABLE TRIGGER ALL;");
			con.commit();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
	
	
	
	private void createBranches(int n) throws Exception {
		FileWriter writer = new FileWriter("csv/branches.csv");
		writer.write("branchid,branchname,balance,address\n");
		for(int i = 1; i <= n; i++) {
			writer.write(i+","+"AutomobileAutomobile,"+ "0,"+
		"jlollduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn\n");
		}
		writer.close();
	}
	
	private void createAccounts(int n) throws Exception {
		FileWriter writer = new FileWriter("csv/accounts.csv");
		writer.write("accid,name,balance,branchid,address\n");
		for(int j=1;j <= n*100000;j++) {
			writer.write(j+","+"AutomobileAutomobile,0,"+((int)(Math.random()*n)+1)+
					",lduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn\n");
			}
		writer.close();
	}
	
	
	private void createTellers(int n) throws Exception {
		FileWriter writer = new FileWriter("csv/tellers.csv");
		writer.write("tellerid,tellername,balance,branchid,address\n");
		for(int j=1;j <= n*100;j++) {
			writer.write(j+","+"AutomobileAutomobile,0,"+((int)(Math.random()*n)+1)+
					",lduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn\n");
			}
		
		writer.close();
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
}
