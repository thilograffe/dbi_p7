package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ControlLoadDrivers {
	static private String address = "jdbc:postgresql://192.168.122.64:5432/postgres";
	private static Connection con;
	//"jdbc:postgresql:postgres" = lokal
	//"jdbc:postgresql://192.168.122.64:5432/postgres" = remote

	public static void main(String[] args) {
		connect();
		deleteHistory();
		disconnect();
		LoadDriver ld1 = new LoadDriver();

	}
	
	private static void deleteHistory() {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("drop table if exists history");
			stmt.close();
			
			stmt = con.createStatement();
			stmt.executeUpdate("create table history\r\n" + 
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
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}

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
}
