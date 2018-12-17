package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.List;
import java.util.PrimitiveIterator.OfInt;

public class LoadDriver implements Runnable {
	Connection con;
	int nummer;
	 OfInt newAccId = new Random().ints(1,10000000).iterator();
	 OfInt newTellerId =  new Random().ints(1,1000).iterator();
	 OfInt newBranchId = new Random().ints(1,100).iterator();
	 OfInt newDelta = new Random().ints(1,10000).iterator();
	static final String address = "jdbc:postgresql://192.168.122.64:5432/postgres";
	//"jdbc:postgresql:postgres" = lokal
	//"jdbc:postgresql://192.168.122.64:5432/postgres" = remote
	PreparedStatement selectBranchBalance, selectAccBalance, selectTellBalance, selectCount;
	//PreparedStatement updateAccBalance, updateBranchBalance, updateTellBalance;
	PreparedStatement insertHistory;
	List<Integer> anzahlTx;

	
	LoadDriver(int nummer, List<Integer> anzahlTx){
		this.nummer = nummer;
		this.anzahlTx = anzahlTx;
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
		anzahlTx.add(anzTrans);
		disconnect();
	}
	
	/**
	 * Erzeugt eine neue Transaktion.
	 * Waehlt mit einer Wahrscheinlichkeit von 0.35 getBalance(), mit 0.5 deposit()
	 * und mit 0.15 analyse() aus.
	 * Wartet anschliessend 50ms. 
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
				//e.printStackTrace();
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
			
			
			selectAccBalance = con.prepareStatement("SELECT balance , accid FROM accounts WHERE accid = ? FOR UPDATE",ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
			selectBranchBalance = con.prepareStatement("SELECT balance, branchid FROM branches WHERE branchid = ? FOR UPDATE",ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
			selectTellBalance = con.prepareStatement("SELECT balance ,tellerid FROM tellers WHERE tellerid = ? FOR UPDATE",ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
			selectCount = con.prepareStatement("SELECT count(delta) FROM history WHERE delta = ? ");
			
			/*updateAccBalance = con.prepareStatement("UPDATE accounts SET balance = ? WHERE accid =?");
			updateBranchBalance = con.prepareStatement("UPDATE branches SET balance = ? WHERE branchid = ?");
			updateTellBalance = con.prepareStatement("UPDATE tellers SET balance = ? WHERE tellerid = ?");
			*/
			insertHistory = con.prepareStatement("INSERT INTO history values(?, ?, ?, ?, ?, " +
				"'Lorem ipsum dolor sit amet, co')");
			
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
		selectBranchBalance.setInt(1, branchId);
		ResultSet rs = selectBranchBalance.executeQuery();
		
		rs.next();
		balance = rs.getInt(1);
		//System.out.println("BranchId: "+branchId+"\n old Balance: "+ rs.getInt(1)+"\n delta: "+delta);
		//balance += delta;
		rs.updateInt(1, balance + delta);
		rs.updateRow();
		rs.close();
		
		/*updateBranchBalance.setInt(1, balance);
		updateBranchBalance.setInt(2, branchId);
		updateBranchBalance.executeUpdate();*/
		
		//update tellers
		selectTellBalance.setInt(1, tellerId);
		rs = selectTellBalance.executeQuery();
		
		rs.next();
		balance = rs.getInt(1);
		rs.updateInt(1, balance+delta);
		//balance += delta;
		rs.close();
		
		/*updateTellBalance.setInt(1, balance);
		updateTellBalance.setInt(2, tellerId);
		updateTellBalance.executeUpdate();*/
		
		//update accounts
		selectAccBalance.setInt(1, accId);
		rs = selectAccBalance.executeQuery();
		
		rs.next();
		balance = rs.getInt(1);
		//balance += delta;
		rs.updateInt(1, balance+delta);
		rs.updateRow();
		rs.close();
		
		/*updateAccBalance.setInt(1, balance);
		updateAccBalance.setInt(2, accId);
		updateAccBalance.executeUpdate();*/
		
		//insert into history
		insertHistory.setInt(1, accId);
		insertHistory.setInt(2, tellerId);
		insertHistory.setInt(3, delta);
		insertHistory.setInt(4, branchId);
		insertHistory.setInt(5, balance);
		insertHistory.executeUpdate();
		
		con.commit();
		
		return balance;
	}
	
	public int analyse(int delta) throws SQLException {
		int count = -1;
		
		selectCount.setInt(1, delta);
		ResultSet rs = selectCount.executeQuery();
		rs.next();
		count = rs.getInt(1);
		
		rs.close();
		
		con.commit();
		
		return count;
	}

}
