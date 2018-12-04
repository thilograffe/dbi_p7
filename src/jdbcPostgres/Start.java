package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class Start implements Runnable{
	private Connection con;
	static final int SCALEACC = 100000;	 //Faktor für die Tabelle accounts
	static final int SCALETEL = 10;		 //Faktor für die Tabelle tellers
	
	//Skalierungsfaktor
	private int n = 10;
	
	//Größe der Bündel
	private int batchSize = 10000;
	
	//Anzahl Threads
	private int threadCount= 4;
	
	//Anzahl Threads
	private int threadIndex= 0;
	
	Start(int mode, int anzahl, int pbatchSize, int anzahlThreads, int pthreadIndex){
		n=anzahl;
		batchSize=pbatchSize;
		threadCount=anzahlThreads;
		threadIndex=pthreadIndex;
		
		/*//mode==0 ist Modus für das Löschen und Erstellen der Tabellen.
		if(mode==0) {
			connect();
			dropTables();
			createTables();
			//Table triggers werden ausgeschaltet
			try {
				PreparedStatement stmt = con.prepareStatement(
					"ALTER TABLE accounts DISABLE TRIGGER ALL;\r\n" + 
					"ALTER TABLE branches DISABLE TRIGGER ALL;\r\n" + 
					"ALTER TABLE tellers DISABLE TRIGGER ALL;");
				stmt.executeUpdate();
				stmt.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}*/
	}
	public void initialize() {
		connect();
		dropTables();
		createTables();
		//Table triggers werden ausgeschaltet
		try {
			PreparedStatement stmt = con.prepareStatement(
				"ALTER TABLE accounts DISABLE TRIGGER ALL;\r\n" + 
				"ALTER TABLE branches DISABLE TRIGGER ALL;\r\n" + 
				"ALTER TABLE tellers DISABLE TRIGGER ALL;");
			stmt.executeUpdate();
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		disconnect();
	}
	
	public void run(){
		System.out.println("Thread "+threadIndex+" ist wirklich gestartet.");
		connect();
		insertIntoNtpsDatabase();
		disconnect();
	}
	
	/*public void main(String[] args) throws SQLException {
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
	*/
	
	//Verbindungsaufbau mit dem Datenbankserver.
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
	
	//Verbindungsabbruch mit dem Datenbankserver.
	public void disconnect() {
		try {
			con.close();
			System.out.println("Disconnected!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Inizialisierung der Datenbank. Tabellen werden angelegt.
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
	
	//Löschen der Datenbank. Bereits existierende Tabellen werden gelöscht. 
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
	
	//ntps-Datenbank wird auf dem Datenbankmanagmentsystem erzeugt. Der Skalierungsfaktor wird als Parameter übergeben.
	public void insertIntoNtpsDatabase() {
		long start = System.currentTimeMillis();
		try {
			PreparedStatement stmt=null;
			if(threadIndex==0) {
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
					stmt.setInt(4, (int)(Math.random()*n)+1);
					stmt.setString(5, "lduvxjffonasgwrnwhwmejokonginaobpcuyfyboquqqgknqjtllvewiheodziqjkrkn");
					stmt.addBatch();
				}
				stmt.executeBatch();
			}
			stmt.close();
			//Tabelle history wird nicht gefüllt.
			if(threadIndex==0) {
				//Tabelle tellers wird gefüllt.
				stmt = con.prepareStatement("insert into tellers values (?, ?, ?, ?, ?)");
			
				for(int i = 1; i <= SCALETEL *n; i++) {
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
			
				//Table triggers werden wieder eingeschaltet
				/*stmt = con.prepareStatement(
					"ALTER TABLE accounts ENABLE TRIGGER ALL;\r\n" + 
					"ALTER TABLE branches ENABLE TRIGGER ALL;\r\n" + 
					"ALTER TABLE tellers ENABLE TRIGGER ALL;");
				stmt.executeUpdate();
				stmt.close();
				*/
			}
				
			//Es wird einmal final ein commit ausgeführt.
			con.commit();
			
			//Das Benchmark-Ergebnis wird ausgegeben.
			System.out.println(System.currentTimeMillis() - start);
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
