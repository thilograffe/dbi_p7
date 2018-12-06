package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class TaskClass implements Runnable{
	//Konstanten
	static final int SCALEACC = 100000;	 //Faktor für die Tabelle accounts
	static final int SCALETEL = 10;		 //Faktor für die Tabelle tellers
	
	//Statische Variablen
	static private int n = 10; //Skalierungsfaktor
	static private int batchSize = 10000; //Größe der Bündel
	static private int threadCount= 4; //Anzahl Threads
	static private int[] zufallszahlen; //Array für vorgenerierte Zufallszahlen von 1 bis n
	static private String address = "jdbc:postgresql://192.168.122.64:5432/postgres";
	//"jdbc:postgresql:postgres" = lokal
	//"jdbc:postgresql://192.168.122.64:5432/postgres" = remote
	
	//Instanzabhängige Variablen
	private int threadIndex; //Anzahl Threads
	private Connection con; //Verbindung zum DBMS
	
	TaskClass(int pthreadIndex){ //Konstruktur für Instanzen, die von Threads bearbeitet werden sollen
		threadIndex=pthreadIndex;
	}
	TaskClass(){ //Konstruktur für Instanzen, die konfigurative Aufgaben haben
	}
	
	public static void configure(int anzahl, int pbatchSize, int anzahlThreads) { //Konfigurationsfunktion zum Setzen der statischen Variablen und der Vorgenerierung der Zufallszahlen
		//Statische Variablen setzen
		n=anzahl;
		batchSize=pbatchSize;
		threadCount=anzahlThreads;
		
		//Vorgenerierung von Zufallszahlen
		int m = n*(SCALEACC+SCALETEL);
		zufallszahlen = new int[m];
		for(int i=0;i<m;i++) {
			zufallszahlen[i] = (int)(Math.random()*n)+1;
		}
	}
	public void initialize() { //Löschen und Erstellen der Tabellen
		connect();
		dropTables();
		createTables();
		disconnect();
	}
	
	public void setTableTriggers(boolean bool) {
		connect();
		if(bool) {
			//Table triggers werden eingeschaltet
			try {
				Statement stmt = con.createStatement();
				stmt.executeUpdate(	
						"ALTER TABLE accounts ENABLE TRIGGER ALL;\r\n" + 
						"ALTER TABLE branches ENABLE TRIGGER ALL;\r\n" + 
						"ALTER TABLE tellers ENABLE TRIGGER ALL;");
				stmt.close();
				con.commit();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
		else {
			//Table triggers werden ausgeschaltet
			try {
				Statement stmt = con.createStatement();
				stmt.executeUpdate(
						"ALTER TABLE accounts DISABLE TRIGGER ALL;\r\n" + 
						"ALTER TABLE branches DISABLE TRIGGER ALL;\r\n" + 
						"ALTER TABLE tellers DISABLE TRIGGER ALL;");
				stmt.close();
				con.commit();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
		disconnect();
	}
	
	public void run(){ //Diese Funktion wird aufgerufen, wenn der zugehörige Thread gestartet wird
		connect();
		insertIntoNtpsDatabase();
		disconnect();
	}
	
	public void connect() { //Verbindungsaufbau mit dem Datenbankserver.
		try {
			con = DriverManager.getConnection(address, "postgres", "datenbank");
			con.setAutoCommit(false);
			System.out.println("Verbunden!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void disconnect() { //Verbindungsabbruch mit dem Datenbankserver.
		try {
			con.close();
			System.out.println("Disconnected!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void createTables() { //Tabellen werden angelegt.
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
	
	public void dropTables() { //Löschen der Datenbank. Bereits existierende Tabellen werden gelöscht. 
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
	
	//ntps-Datenbank wird auf dem Datenbankmanagmentsystem gefüllt.
	public void insertIntoNtpsDatabase() {
		try {
			PreparedStatement stmt=null;
			
			if(threadIndex==0) { //Nur der erste Thread erstellt diese Tabelle
				//Tabelle branches wird gefüllt.
				stmt = con.prepareStatement("insert into branches values (?,?,?,?)");
			
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
			}
			
			//Tabelle accounts wird gefüllt.
			stmt = con.prepareStatement(
					"insert into accounts values (?, ?, ?, ?, ?)");
			
			for(int i = 0; i < (SCALEACC*n)/threadCount/batchSize; i++) {
				for(int j=0;j<batchSize;j++) {
					stmt.setInt(1, i*batchSize+j+(threadIndex*(SCALEACC*n/threadCount))+1);
					stmt.setString(2, "AutomobileAutomobile");
					stmt.setInt(3, 0);
					stmt.setInt(4, zufallszahlen[i*batchSize+j+(threadIndex*(SCALEACC*n/threadCount))]);
					stmt.setString(5, "lduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn");
					stmt.addBatch();
				}
				stmt.executeBatch();
			}
			stmt.close();
			//Tabelle history wird nicht gefüllt.
			
			if(threadIndex==0) { //Nur der erste Thread erstellt diese Tabelle
				//Tabelle tellers wird gefüllt.
				stmt = con.prepareStatement("insert into tellers values (?, ?, ?, ?, ?)");
			
				for(int i = 1; i <= SCALETEL *n; i++) {
					stmt.setInt(1, i);
					stmt.setString(2, "AutomobileAutomobile");
					stmt.setInt(3, 0);
					stmt.setInt(4, zufallszahlen[n*SCALEACC+i-1]);
					stmt.setString(5, "lduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn");
					stmt.addBatch();
				}
				//Da in die Tabelle branches nicht mehr Tupel eingefügt werden als unsere willkürliche batchSize groß ist, wird nur ein Batch erstellt.
				stmt.executeBatch();
				stmt.close();
			
			}
			//Es wird einmal final ein commit (pro thread) ausgeführt.
			con.commit();
			
			ControlClass.callback(threadIndex); //Ein Callback wird an die ControlClass geschickt.
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
