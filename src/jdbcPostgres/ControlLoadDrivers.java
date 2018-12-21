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
 * Diese Klasse erzeugt in 5 Threads LoadDriver Instanzen, welche Last auf die 
 * Datenbank generieren.
 *
 */
public class ControlLoadDrivers {
	static private String address;
	private static Connection con;
	public List<Integer> anzahlTx;

	public static void main(String[] args) {
		new ControlLoadDrivers();
	}
	
	private ControlLoadDrivers() {
		// Liste für die Gesamtanzahl der Transaktionen; Liste wird an jeden 
		// LoadDriver übergeben
		anzahlTx = Collections.synchronizedList(new ArrayList<Integer>());
		address=LoadDriver.address;
		connect();
		deleteHistory();
		disconnect();
		
		// Erstellen der 5-LoadDriver-Threads
		for(int i = 1; i<=5; i++) {
			new Thread(new LoadDriver(i, anzahlTx)).start();
		}
		
		try {
			// 5 Sekuden länger als 10 Minuten schlafen, da auf die Mess-
			// ergebnisse aller Threads gewartet werden muss 
			Thread.sleep(605000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		long gesTx = 0;
		
		// erneutes Warten auf die Messergebnisse
		while (anzahlTx.isEmpty()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		// Warten aus Sicherheit, dass wirklich alle Messergebnisse da sind
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		for(int x:anzahlTx) {
			gesTx += x;
		}
		// Berechnung der druchschnittlichen Transaktionen pro Sekunde
		double txPerSc = ((double)gesTx)/300;
		System.out.println("Ges tx: " +gesTx+"\n Tx per sec: "+ txPerSc);
		
	}
	
	/**
	 * entfernt zu Beginn jeder Messung die history-Tabelle und fügt sie 
	 * sofort wieder ein
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
			System.out.println("History-Relation wurde geleert! ");
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
			System.out.println("Kontroll-Klasse verbunden!");
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
			System.out.println("Kontroll-Klasse hat Verbindung zur Datenbank "
					+ "getrennt und startet nun die 5 Threads!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
}
