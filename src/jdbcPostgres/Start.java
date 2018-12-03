package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class Start {
	static Connection con;
	static final int scaleAcc = 100000;
	static final int scaleTel = 10;
	static final int batchSize = 10000;
	
	//Mode wird auf 0 gesetzt um die Datenbank neu zu erzeugen und gegebenenfalls vorher zu löschen. Bei 1 wird die ntps-Datenbank gefüllt.
	static final int mode = 	1;
	
	//Anzahl ist unser Skalierungsfaktor und wird bei der Methode insertIntoNtpsDatabase(anzahl) als parameter übergeben.
	static final int anzahl = 	50;
	
	public static void main(String[] args) throws SQLException {
		connect();
		
		//mode==0 : Das ist der Modus für das Löschen und Erstellen der Tabellen.
		if(mode==0) {
			dropTables();
			createTables();
		}
		//mode==1 : Das ist der Modus für das Füllen der Tabellen.
		else if(mode==1) {
			insertIntoNtpsDatabase(anzahl);
		}
		disconnect();
	}
	
	//Verbindungsaufbau mit dem Datenbankserver.
	public static void connect() {
		try {
			con = DriverManager.getConnection("jdbc:postgresql:postgres", "postgres", "datenbank");
			con.setAutoCommit(false);
			System.out.println("Verbunden!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Verbindungsabbruch mit dem Datenbankserver.
	public static void disconnect() {
		try {
			con.close();
			System.out.println("Disconnected!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Inizialisierung der Datenbank. Tabellen werden angelegt.
	public static void createTables() {
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
	
	//Löschen der Datenbank. Bereits existierende Tabellen werden gelöscht. 
	public static void dropTables() {
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
	
	//ntps-Datenbank wird auf dem Datenbankmanagmentsystem erzeugt. Der Skalierungsfaktor wird als Parameter übergeben.
	public static void insertIntoNtpsDatabase(int n) {
		long start = System.currentTimeMillis();
		try {
			//Table triggers werden ausgeschaltet
			PreparedStatement stmt = con.prepareStatement(
				"ALTER TABLE accounts DISABLE TRIGGER ALL;\r\n" + 
				"ALTER TABLE branches DISABLE TRIGGER ALL;\r\n" + 
				"ALTER TABLE tellers DISABLE TRIGGER ALL;");
			stmt.executeUpdate();
			stmt.close();

			//Tabelle branches wird gefüllt.
			stmt = con.prepareStatement(
					"insert into branches values (?,?,?,?)");
			
			for(int i = 1; i <= n; i++) {
				stmt.setInt(1, i);
				stmt.setString(2, "AutomobileAutomobile");
				stmt.setInt(3, 0);
				stmt.setString(4, "jlollduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn");
				stmt.addBatch();
			}
			//Da in die Tabelle branches nicht mehr Tupel eingefügt werden als unsere willkürliche batchSize groß ist, wird nur ein Batch erstellt.
			stmt.executeBatch();
			stmt.close();
			
			//Tabelle accounts wird gefüllt.
			stmt = con.prepareStatement(
					"insert into accounts values (?, ?, ?, ?, ?)");
			
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
			
			//Tabelle tellers wird gefüllt.
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
			//Da in die Tabelle branches nicht mehr Tupel eingefügt werden als unsere willkürliche batchSize groß ist, wird nur ein Batch erstellt.
			stmt.executeBatch();
			stmt.close();
			
			//Tabelle history wird nicht gefüllt.
			
			//Table triggers werden wieder eingeschaltet
			stmt = con.prepareStatement(
					"ALTER TABLE accounts ENABLE TRIGGER ALL;\r\n" + 
					"ALTER TABLE branches ENABLE TRIGGER ALL;\r\n" + 
					"ALTER TABLE tellers ENABLE TRIGGER ALL;");
			stmt.executeUpdate();
			stmt.close();
				
			//Es wird einmal final ein commit ausgeführt.
			con.commit();
			
			//Das Benchmark-Ergebnis wird ausgegeben.
			System.out.println(System.currentTimeMillis() - start);
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
