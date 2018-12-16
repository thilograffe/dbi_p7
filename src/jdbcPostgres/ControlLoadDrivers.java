package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 * @author Gruppe DBI32
 * 
 * Diese Klasse erzeugt in 5 Threads LoadDriver Instanzen, welche Last auf die Datenbank generieren.
 *
 */
public class ControlLoadDrivers {
	static private String address;
	private static Connection con;

	public static void main(String[] args) {
		address=LoadDriver.address;
		connect();
		deleteHistory();
		disconnect();
		new Thread(new LoadDriver(1)).start();
		new Thread(new LoadDriver(2)).start();
		new Thread(new LoadDriver(3)).start();
		new Thread(new LoadDriver(4)).start();
		new Thread(new LoadDriver(5)).start();
	}
	
	/**
	 * entfernt zu Beginn jeder Messung die history-Tabelle und fügt sie sofort wieder ein
	 */
	private static void deleteHistory() {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("drop table if exists history");
			stmt.close();
			
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
			stmt.close();
			con.commit();
			System.out.println("Dropped history-table and immediately created a new one.");
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
