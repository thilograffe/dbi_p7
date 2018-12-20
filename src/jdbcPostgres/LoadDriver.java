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

/**
 * 
 * @author Gruppe DBI32
 * 
 */
public class LoadDriver implements Runnable {
	Connection con;
	int nummer;
	
	// Erzeugen der Zufallszahlen 
	OfInt newAccId = new Random().ints(1,10000000).iterator();
	OfInt newTellerId =  new Random().ints(1,1000).iterator();
	OfInt newBranchId = new Random().ints(1,100).iterator();
	OfInt newDelta = new Random().ints(1,10000).iterator();
	 
	static final String address = 
		"jdbc:postgresql://192.168.122.42:5432/postgres";
	//"jdbc:postgresql:postgres" = lokal
	//"jdbc:postgresql://192.168.122.64:5432/postgres" = remote
	
	PreparedStatement selectAccBalance, selectCount;
	
	// Liste für die Anazhl aller Transaktionen
	List<Integer> anzahlTx;
	
	public LoadDriver(int nummer, List<Integer> anzahlTx){
		this.nummer = nummer;
		this.anzahlTx = anzahlTx;
	}
	@Override
	public void run() {
		connect();
		
		int anzTrans = 0;
		long timer = System.currentTimeMillis();
		
		while(true) {
			// vergangene Zeit
			long aktZeit = System.currentTimeMillis() - timer;
			
			// Lastzeitraum 10 Minuten (=> 600000 ms)
			if(aktZeit >600000) {
				break;
			}
			neueTransaktion();
			
			// Beginn der tps-Messung
			if(aktZeit>240000 && aktZeit<540000) {
				anzTrans++;
			}
			
		}
		// Übermitteln der Gesamtanzahl an Transaktionen
		anzahlTx.add(anzTrans);
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
			try {
				deposit(newAccId.next() , newTellerId.nextInt(),
						newBranchId.nextInt(), newDelta.nextInt());
			} catch (SQLException e) {
				try {
					con.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		}
		else {
			try {
				analyse(newDelta.nextInt());
			} catch (SQLException e) {
				try {
					con.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
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
			con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			System.out.println("Verbunden!");
			
			// Erstellen der PreparedStatements
			selectAccBalance = con.prepareStatement("SELECT balance, accid FROM"
					+ " accounts WHERE accid = ?");
			selectCount = con.prepareStatement("SELECT count(delta) FROM history"
					+ " WHERE delta = ? ");
			
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Verbindungsabbruch mit dem Datenbankserver
	 */
	public void disconnect() {
		try {
			// Schließen der PerparedStatemnts
			selectAccBalance.close();
			selectCount.close();
			
			con.close();
			System.out.println(this.nummer + " Disconnected!");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Kontostands-TX
	 * 
	 * @param accId Kontonummer
	 * @return Kontostand passend zur Kontonummer
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
				con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
		return balance;
	}
	
	/**
	 * Einzahlungs-TX
	 * 
	 * @return aktualisierter Kontostand passend zur Kontonummer
	 * @throws SQLException
	 */
	public int deposit(int accId, int tellerId, int branchId, int delta) 
			throws SQLException {
		// Erstellen des CallableStatements
		CallableStatement stmt = con.prepareCall("{call deposit(?,?,?,?)}");
		
		// Setzen der Parameter
		stmt.setInt(1, accId);
		stmt.setInt(2, tellerId);
		stmt.setInt(3, branchId);
		stmt.setInt(4, delta);
		stmt.registerOutParameter(1, Types.INTEGER);
		stmt.execute();
		con.commit();
		return stmt.getInt(1);
	}
	
	/**
	 * Analyse-TX
	 * 
	 * @param delta Einzahlungsbetrag
	 * @return Anzahl der Einzahlungen mit dem entsprechenden Betrag
	 * @throws SQLException
	 */
	public int analyse(int delta) throws SQLException {
		int count = -1;
		
		// Setzen des Parameters des PreparedStatements
		selectCount.setInt(1, delta);
		ResultSet rs = selectCount.executeQuery();
		rs.next();
		count = rs.getInt(1);
		
		rs.close();
		
		con.commit();
		
		return count;
	}

}
