import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class IgniteTestDB {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // Register driver
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Return connection to the cluster (Port 10800 default for JDBC client)
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800");

        // Create tables
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS TA; DROP TABLE IF EXISTS TB");
        stmt.executeUpdate("CREATE TABLE TA ( ID INT PRIMARY KEY, NAME VARCHAR, AGE INT)");
        stmt.executeUpdate("CREATE TABLE TB ( ID INT PRIMARY KEY, IDOFTA INT)");

        // Metadata table
        stmt.executeUpdate("DROP TABLE IF EXISTS FRAGMETA; DROP TABLE IF EXISTS COMETA;");
        stmt.executeUpdate("CREATE TABLE FRAGMETA (ID INT PRIMARY KEY, TABLE VARCHAR, ATTRIBUTE VARCHAR, " +
                "MINVALUE INT, MAXVALUE INT) WITH \"template=replicated,backups=0\"");

        stmt.executeUpdate("CREATE TABLE COMETA (ID INT PRIMARY KEY, TABLE VARCHAR, JOINATTR VARCHAR, " +
                "COTABLE VARCHAR, COJOIN VARCHAR) WITH \"template=replicated,backups=0\"");
    }
}
