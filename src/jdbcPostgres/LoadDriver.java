package jdbcPostgres;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Random;
import java.util.List;
import java.util.PrimitiveIterator.OfInt;

public class LoadDriver implements Runnable {
	Connection con;
	int nummer;
	//Erstellen von zufaelligen Zahlen
	 OfInt newAccId = new Random().ints(1,10000000).iterator();
	 OfInt newTellerId =  new Random().ints(1,1000).iterator();
	 OfInt newBranchId = new Random().ints(1,100).iterator();
	 OfInt newDelta = new Random().ints(1,10000).iterator();
	static final String address = "jdbc:postgresql://192.168.178.23:5432/postgres";
	//"jdbc:postgresql:postgres" = lokal
	//"jdbc:postgresql://192.168.122.64:5432/postgres" = remote
	PreparedStatement selectAccBalance, selectCount;
	List<Integer> anzahlTx;


	
	LoadDriver(int nummer, List<Integer> anzahlTx){
		this.nummer = nummer;
		this.anzahlTx = anzahlTx;//Liste, die Anzahl aller Transaktionen enthält 
	}
	
	@Override
	public void run() {
		connect();
		int anzTrans = 0;
		long timer = System.currentTimeMillis();
		while(true) {
			//Zeit, die seit Start vergangen ist
			long aktZeit = System.currentTimeMillis() - timer;
			if(aktZeit >600000) {//Bei 10 min Ende
				break;
			}
			neueTransaktion();
			if(aktZeit>240000 && aktZeit<540000) {//Zeitraum der Messung
				anzTrans++;//Zaehlen der Transaktionen
			}
			
		}
		anzahlTx.add(anzTrans);//Anzahl der Transaktionen eintragen
		disconnect();
	}
	
	/**
	 * Erzeugt eine neue Transaktion.
	 * Waehlt mit einer Wahrscheinlichkeit von 0.35 getBalance(),
	 * mit 0.5 deposit() und mit 0.15 analyse() aus.
	 * Wartet anschliessend 50ms. 
	 */
	private void neueTransaktion() {
		double x = Math.random();
		if(x<0.35) {
			getBalance(newAccId.nextInt());
		}
		else if(x<0.85) {
			deposit(newAccId.next() , newTellerId.nextInt(),
					newBranchId.nextInt(), newDelta.nextInt());
		}
		else {
			analyse(newDelta.nextInt());
		}
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void connect() {
		try {
			con = DriverManager.getConnection(address, "dbi", "dbi");
			con.setAutoCommit(false);
			//
			con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			System.out.printf("LoadDriver %d verbunden!\n",nummer);
			
			//Erstellen aller PreparedStatements 
			selectAccBalance = con.prepareStatement("SELECT balance , accid "+
												"FROM accounts WHERE accid = ?");
			selectCount = con.prepareStatement("SELECT count(delta) "+
												"FROM history WHERE delta = ? ");			
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Verbindungsabbruch mit dem Datenbankserver.
	public void disconnect() {
		try {
			
			con.close();
			System.out.printf("Loaddriver %d disconnected!\n", nummer);
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Kontostand-TX Methode
	 */
	public int getBalance(int accId) {
		int balance = -1;
		
		try {
			selectAccBalance.setInt(1, accId);
			ResultSet rs = selectAccBalance.executeQuery();
			rs.next();
			balance = rs.getInt(1);
			rs.close();
			
			con.commit();
		}
		catch (SQLException e) {
			try {
				//Rollback, wenn ein Fehler auftritt
				con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
		return balance;
	}
	
	/**
	 * Einzahlungs-TX Methode
	 */
	public int deposit(int accId, int tellerId, int branchId, int delta){
		boolean a = true;
		 while(a){
			 try {
			//Aufruf der stored procedure
			CallableStatement stmt = con.prepareCall("{call deposit(?,?,?,?)}");
			//Setzten der Parameter
			stmt.setInt(1, accId);
			stmt.setInt(2, tellerId);
			stmt.setInt(3, branchId);
			stmt.setInt(4, delta);
			stmt.registerOutParameter(1, Types.INTEGER);
			stmt.execute();
			con.commit();
			//Rückgabe des Kontostandes
			return stmt.getInt(1);
			} catch (SQLException e) {
				try {
					//Rollback, wenn ein Fehler auftritt
					con.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				//e.printStackTrace();
			}
		}
		return 0;
	}
	
	/**
	 * Analyse-TX Methode
	 */
	public int analyse(int delta) {
		int count = -1;
		try {
			selectCount.setInt(1, delta);
			ResultSet rs = selectCount.executeQuery();
			rs.next();
			count = rs.getInt(1);
			
			rs.close();
			
			con.commit();
		} catch (SQLException e) {
			try {
				//Rollback, wenn ein Fehler auftritt
				con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		return count;
	}

}
