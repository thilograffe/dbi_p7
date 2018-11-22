package jdbcPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Start {
	Connection con;
	public static void main(String[] args) throws SQLException {
		Start start = new Start();
		start.connect();
		start.createSQLPreparedStatement();
		start.con.close();
	}
	
	public void connect() {
		try {
			con = DriverManager.getConnection("jdbc:postgresql:fh_dbi", "dbi", "dbi");
			System.out.println("Verbunden!\n");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void createSQLPreparedStatement() {
		System.out.println("");
		Scanner in = new Scanner(System.in);
		String pid = in.nextLine();
		try {
			PreparedStatement stmt = con.prepareStatement(
				"");
			stmt.setString(1, pid);
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()) {
				System.out.println(rs.getString(""));
			}
			stmt.close();
			rs.close();
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
