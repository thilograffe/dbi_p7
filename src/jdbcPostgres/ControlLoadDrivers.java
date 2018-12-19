package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 
 * @author Gruppe DBI32
 * 
 * Diese Klasse erzeugt in 5 Threads LoadDriver Instanzen,
 *  welche Last auf die Datenbank generieren.
 */
public class ControlLoadDrivers {
	static private String address;
	private static Connection con;
	public List<Integer> anzahlTx;

	public static void main(String[] args) {
		new ControlLoadDrivers();
	}
	
	private ControlLoadDrivers() {
		//synchronizedList, fuer die Anzahl der gesammten Transaktionen
		anzahlTx = Collections.synchronizedList(new ArrayList<Integer>());
		address=LoadDriver.address;
		connect();
		deleteHistory();
		disconnect();
		
		for(int i = 1;i<=5;i++) {
			new Thread(new LoadDriver(i, anzahlTx)).start();
		}
		try {
			Thread.sleep(605000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long gesTx = 0;
		
		while (anzahlTx.isEmpty()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(int x:anzahlTx) {
			gesTx += x;
		}
		double txPerSc = ((double)gesTx)/300;
		System.out.println("Ges tx: " +gesTx+"\n Tx per sec: "+ txPerSc);
		
	}
	
	/**
	 * Entfernt zu Beginn jeder Messung die history-Tabelle
	 *  und fuegt sie sofort wieder ein.
	 */
	private static void deleteHistory() {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("drop table if exists history");
			stmt.close();
			con.commit();
			stmt = con.createStatement();
			stmt.executeUpdate(
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
			
			//Stored procedure erstellen
			stmt.execute("CREATE OR REPLACE FUNCTION deposit("+
						 "IN acId INTEGER,IN telId INTEGER,"+
						 "IN braId INTEGER,IN delta INTEGER) "+
						 "RETURNS INTEGER AS $$\r\n" + 
						 "DECLARE\r\n" + 
						 "oldBal Integer;\r\n" + 
						 "BEGIN\r\n" + 
						 "UPDATE branches SET balance = balance"+
						 "+delta WHERE branchid =braId;\r\n" + 
						 "UPDATE tellers SET balance = "+
						 "balance+delta WHERE tellerid =telId;\r\n" + 
						 "SELECT balance INTO oldBal "+
						 "FROM accounts WHERE accid = acId;\r\n" + 
						 "oldBal = (oldBal+delta);\r\n" + 
						 "UPDATE accounts SET balance = oldBal "+
						 "WHERE accid =acId;\r\n" + 
						 "INSERT INTO history values(acId,telId,"+
						 "delta,braId,oldBal,"+
						 "'Lorem ipsum dolor sit amet, co');\r\n" + 
						 "RETURN oldBal;\r\n" + 
						 "END;\r\n" + 
						 "$$ LANGUAGE plpgsql;");
			stmt.close();
			
			con.commit();
			System.out.println("Dropped history-table and"+
								"immediately created a new one.");
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Verbindungsaufbau mit dem Datenbankserver
	 */
	public static void connect() {
		try {
			con = DriverManager.getConnection(address, "dbi", "dbi");
			con.setAutoCommit(false);
			System.out.println("Verbunden!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Verbindungsabbruch mit dem Datenbankserver
	 */
	public static void disconnect() {
		try {
			con.close();
			System.out.println("Disconnected!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
}
