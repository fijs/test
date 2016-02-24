/**
 * Fernando I Jaime, A11643783

 * This Java program exemplifies the basic usage of JDBC.
 * Requirements:
 * (1) JDK 1.6+.
 * (2) SQLite3.
 * (3) SQLite3 JDBC jar (https://bitbucket.org/xerial/sqlitejdbc/downloads/sqlite-jdbc-3.8.7.jar).
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PA2 {
    public static void main(String[] args) {
        Connection conn = null; // Database connection.
        try {
            // Load the JDBC class.
            Class.forName("org.sqlite.JDBC");
            // Get the connection to the database.
            // - "jdbc" : JDBC connection name prefix.
            // - "sqlite" : The concrete database implementation
            // (e.g., sqlserver, postgresql).
            // - "pa2.db" : The name of the database. In this project,
            // we use a local database named "pa2.db". This can also
            // be a remote database name.
            conn = DriverManager.getConnection("jdbc:sqlite:/Users/isaac/IdeaProjects/PA2/src/pa2.db");
            System.out.println("Opened database successfully.");

            // Use case #1: Create and populate a table.
            // Get a Statement object.
            Statement stmt = conn.createStatement();

            stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
            stmt.executeUpdate("DROP TABLE IF EXISTS T;");
            stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");

            stmt.executeUpdate("CREATE TABLE T AS " +
                    "SELECT Airline, Origin, Destination, 0 AS Stops FROM Flight;");
            stmt.executeUpdate("CREATE TABLE Delta AS " +
                    "SELECT Airline, Origin, Destination, 0 AS Stops FROM Flight;");

            ResultSet D = stmt.executeQuery("SELECT * FROM Delta;");
            ResultSet C = stmt.executeQuery("SELECT COUNT(*) AS count FROM Delta;");

            // System.out.println ("\nCounter: " + C.getString(1));

            while (C.getInt("count") != 0) {

                stmt.executeUpdate("DROP TABLE IF EXISTS T_Old;");
                stmt.executeUpdate("CREATE TABLE T_Old AS SELECT * FROM T;");

                stmt.executeUpdate("DROP TABLE IF EXISTS T;");
                stmt.executeUpdate("CREATE TABLE T AS SELECT * FROM T_Old UNION" +
                        " SELECT F.Airline, F.Origin, D.Destination, D.Stops+1" +
                        " FROM Flight F, Delta D" +
                        " WHERE F.Destination = D.Origin AND F.Airline = D.Airline AND F.Origin != D.Destination" +
                        " AND NOT EXISTS (SELECT * FROM T_Old K WHERE D.Destination = K.Destination AND " +
                        " F.Origin = K.Origin AND F.Airline = K.Airline);");

                stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
                stmt.executeUpdate("CREATE TABLE Delta AS SELECT DISTINCT * FROM T EXCEPT SELECT * FROM T_Old;");

                D = stmt.executeQuery("SELECT * FROM T EXCEPT SELECT * FROM T_Old;");
                C = stmt.executeQuery("SELECT COUNT(*) AS count FROM (SELECT * FROM T EXCEPT SELECT * FROM T_Old)c;");
                //System.out.println ("\nCounter: " + C.getInt("count"));

            }

            //System.out.println ("\nCounter: " + C);
            stmt.executeUpdate("CREATE TABLE Connected AS SELECT * FROM T ORDER BY Origin, Destination;");

            /*
            ResultSet T = stmt.executeQuery("SELECT * FROM T ORDER BY Origin, Destination;");
            Print out table T (Connected)
            System.out.println ("\nStatement result:");

            while (T.next()) {
                // Get the attribute value.
                System.out.print(T.getString("Airline"));
                System.out.print(" | ");
                System.out.print(T.getString("Origin"));
                System.out.print(" | ");
                System.out.print(T.getString("Destination"));
                System.out.print(" | ");
                System.out.println(T.getString("Stops"));
            }
            */

            //T.close();

            stmt.executeUpdate("DROP TABLE IF EXISTS T_Old;");
            stmt.executeUpdate("DROP TABLE IF EXISTS T;");
            stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");

            D.close();
            C.close();
            stmt.close();

        } catch (Exception e) {
            throw new RuntimeException("There was a runtime problem!", e);
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(
                        "Cannot close the connection!", e);
            }
        }
    }//end main
}//end class PA2
