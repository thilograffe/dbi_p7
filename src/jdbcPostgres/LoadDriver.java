package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadDriver implements Runnable {
	Connection con;
	static private String address = "jdbc:postgresql://192.168.122.64:5432/postgres";
	//"jdbc:postgresql:postgres" = lokal
	//"jdbc:postgresql://192.168.122.64:5432/postgres" = remote

	public static void main(String[] args) {
		new LoadDriver().run();
	}
	
	@Override
	public void run() {
		connect();
		
		for (long timer = System.currentTimeMillis(); System.currentTimeMillis() - timer <= 600000; ) {
			
		}
		
		disconnect();
	}
	
	public void connect() {
		try {
			con = DriverManager.getConnection(address, "dbi", "dbi");
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
	
	public int getBalance(int accId) {
		int balance = -1;
		
		try {
			Statement stmt = con.createStatement();
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
			e.printStackTrace();
		}
		
		return balance;
	}
	
	public int deposit(int accId, int tellerId, int branchId, int delta) throws SQLException {
		int balance = -1;
		
		//update branches
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(
				"SELECT balance " +
				"FROM branches " +
				"WHERE branchid = " + branchId);
		
		rs.next();
		balance = rs.getInt(1);
		balance += delta;
		rs.close();
		stmt.close();
		
		stmt = con.createStatement();
		stmt.executeUpdate(
				"UPDATE branches " + 
				"SET balance = " + balance + " " +
				"WHERE branchid = " + branchId);
		
		
		stmt.close();
		
		//update tellers
		stmt = con.createStatement();
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
		stmt.executeUpdate(
				"UPDATE tellers " + 
				"SET balance = " + balance + " " +
				"WHERE tellerid = " + tellerId);
		
		
		stmt.close();
		
		//update accounts
		stmt = con.createStatement();
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
		stmt.executeUpdate(
				"UPDATE accounts " + 
				"SET balance = " + balance + " " +
				"WHERE accid = " + accId);
		
		
		stmt.close();
		
		//insert into history
		stmt = con.createStatement();
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
