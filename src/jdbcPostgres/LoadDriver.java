package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.PrimitiveIterator.OfInt;

public class LoadDriver implements Runnable {
	Connection con;
	int nummer;
	 OfInt newAccId = new Random().ints(1,10000000).iterator();
	 OfInt newTellerId =  new Random().ints(1,1000).iterator();
	 OfInt newBranchId = new Random().ints(1,100).iterator();
	 OfInt newDelta = new Random().ints(1,10000).iterator();
	static final String address = "jdbc:postgresql:dbi_p7";
	//"jdbc:postgresql:postgres" = lokal
	//"jdbc:postgresql://192.168.122.64:5432/postgres" = remote

	LoadDriver(int nummer){
		this.nummer = nummer;
	}
	@Override
	public void run() {
		connect();
		
		//erstmal nur 1 Sekunde (=> 1000ms) zum Test
		int anzTrans = 0;
		long timer = System.currentTimeMillis();
		while(true) {
			long aktZeit = System.currentTimeMillis() - timer;
			if(aktZeit >600000) {
				break;
			}
			neueTransaktion();
			if(aktZeit>240000 && aktZeit<540000) {
				anzTrans++;
			}
			
		}
		/*for (long timer = System.currentTimeMillis(); System.currentTimeMillis() - timer <= 10000; ) {
			neueTransaktion();
		}*/
		System.out.println(nummer+": "+(double)((double)(anzTrans)/300));
		disconnect();
	}
	
	/**
	 * Erzeugt eine neue Transaktion.
	 * W�hlt mit einer Wahrscheinlichkeit von 0.35 getBalance(), mit 0.5 deposit()
	 * und mit 0.15 analyse() aus.
	 * Wartet anschlie�end 50ms. 
	 */
	private void neueTransaktion() {
		double x = Math.random();
		if(x<0.35) {
			getBalance(newAccId.nextInt());
		}
		else if(x<0.85) {
			try {
				deposit(newAccId.next() , newTellerId.nextInt(), newBranchId.nextInt(), newDelta.nextInt());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				try {
					con.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}
		else {
			try {
				analyse(newDelta.nextInt());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				try {
					con.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void connect() {
		try {
			con = DriverManager.getConnection(address, "dbi", "dbi");
			con.setAutoCommit(false);
			con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
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
	
	public int getBalance(int accId) {
		int balance = -1;
		
		try {
			Statement stmt = con.createStatement();
			stmt.setQueryTimeout(5);
			ResultSet rs = stmt.executeQuery(
					"SELECT balance " +
					"FROM accounts " +
					"WHERE accid = " + accId);
			
			rs.next();
			balance = rs.getInt(1);
			rs.close();
			stmt.close();
			con.commit();
		}
		catch (SQLException e) {
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
		return balance;
	}
	
	public int deposit(int accId, int tellerId, int branchId, int delta) throws SQLException {
		int balance = -1;
		
		//update branches
		Statement stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		ResultSet rs = stmt.executeQuery(
				"SELECT balance " +
				"FROM branches " +
				"WHERE branchid = " + branchId);
		
		rs.next();
		try {
			balance = rs.getInt(1);
			}catch (SQLException e) {
				System.out.println("Fehler: "+ branchId+"hat balance:"+balance);
				e.printStackTrace();
			}
		
		balance += delta;
		rs.close();
		stmt.close();
		
		stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		stmt.executeUpdate(
				"UPDATE branches " + 
				"SET balance = " + balance + " " +
				"WHERE branchid = " + branchId);
		
		
		stmt.close();
		
		//update tellers
		stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		rs = stmt.executeQuery(
				"SELECT balance " +
				"FROM tellers " +
				"WHERE tellerid = " + tellerId);
		
		rs.next();
		balance = rs.getInt(1);
		balance += delta;
		rs.close();
		stmt.close();
		
		stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		stmt.executeUpdate(
				"UPDATE tellers " + 
				"SET balance = " + balance + " " +
				"WHERE tellerid = " + tellerId);
		
		
		stmt.close();
		
		//update accounts
		stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		rs = stmt.executeQuery(
				"SELECT balance " +
				"FROM accounts " +
				"WHERE accid = " + accId);
		
		rs.next();
		balance = rs.getInt(1);
		balance += delta;
		rs.close();
		stmt.close();
		
		stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		stmt.executeUpdate(
				"UPDATE accounts " + 
				"SET balance = " + balance + " " +
				"WHERE accid = " + accId);
		
		
		stmt.close();
		
		//insert into history
		stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		stmt.executeUpdate(
				"INSERT INTO history values(" + accId + ", " + tellerId +
				", " + delta + ", " + branchId + ", " + balance + ", " +
				"'Lorem ipsum dolor sit amet, co')");
		
		stmt.close();
		
		con.commit();
		
		return balance;
	}
	
	public int analyse(int delta) throws SQLException {
		int count = -1;
		
		Statement stmt = con.createStatement();
		stmt.setQueryTimeout(5);
		ResultSet rs = stmt.executeQuery(
				"SELECT count(delta) " +
				"FROM history " +
				"WHERE delta = " + delta);
		
		rs.next();
		count = rs.getInt(1);
		
		rs.close();
		stmt.close();
		
		con.commit();
		
		return count;
	}

}
