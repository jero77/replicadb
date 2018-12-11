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

        Statement stmt = conn.createStatement();
        stmt.executeUpdate("CREATE TABLE TA ( ID INT PRIMARY KEY, NAME VARCHAR, AGE INT)");
        stmt.executeUpdate("CREATE TABLE TB ( ID INT PRIMARY KEY, IDOFTA INT)");
    }
}
