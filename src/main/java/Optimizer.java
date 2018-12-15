import java.sql.*;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;

import com.facebook.presto.sql.TreePrinter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import org.apache.ignite.*;

public class Optimizer {



    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        /*
            SELECT ...
            FROM TA, TB
            WHERE ... AND TA.x (>|>=|<|<=|=) c AND ...

            Lookup metadata:
                -> If there is a fragmentation of TA on attribute x, find fragment(s) containing matching tuples
                   Furthermore, see if TB is co-partitioned to TA on some join attr.; if so, collect tuples accordingly,
                   else find matching tuples/fragments of TB and copy them to server hosting fragment of TA
                -> If there is no fragmentation of TA on attr. x, ?
         */

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


        // Store some meta-info
        String insert = "INSERT INTO FRAGMETA (ID, TABLE, ATTRIBUTE, MINVALUE, " + "MAXVALUE) VALUES (?, ?, ?, ?, ?)";
        PreparedStatement prep = conn.prepareStatement(insert);
        for (int i = 0; i < 5; i++) {
            // 5 fragments for TA on TA.AGE: 1-20, 21-40, etc.
            prep.setInt(1, i);
            prep.setString(2, "TA");
            prep.setString(3, "AGE");
            prep.setInt(4, 20 * i + 1);
            prep.setInt(5, 20 * (i + 1));
            prep.executeUpdate();
        }

        // Some co-partitioned meta data
        insert = "INSERT INTO COMETA (ID, TABLE, JOINATTR, COTABLE, COJOIN) VALUES (?,?,?,?,?)";
        prep = conn.prepareStatement(insert);
        prep.setInt(1, 0);
        prep.setString(2, "TB");
        prep.setString(3, "IDOFTA");
        prep.setString(4, "TA");
        prep.setString(5, "ID");
        prep.executeUpdate();





        // Query ...
        String sql = "SELECT * FROM TA, TB WHERE TA.AGE <= 35 AND TA.ID = TB.IDOFTA";

        // parsing with presto-parser
        SqlParser parser = new SqlParser();
        Query query = (Query) parser.createStatement(sql, new ParsingOptions());
        QuerySpecification body = (QuerySpecification) query.getQueryBody();
        Optional<Expression> optional = body.getWhere();
        Expression e = optional.orElse(null);
        System.out.println("Where-Expression: " + e);

        // print AST tree
        IdentityHashMap<Expression, QualifiedName> ihm = new IdentityHashMap<Expression, QualifiedName>();
        ihm.put(e, QualifiedName.of(e.toString()));
        TreePrinter tp = new TreePrinter(ihm, System.out);
        tp.print(e);

        // Analyze the where clause
        SelectionConditionAnalyzer sca = new SelectionConditionAnalyzer();
        sca.analyzePrint(e);
        ArrayList<ComparisonExpression> comparisons = sca.getComparisons();

        // TODO match comparisons from WHERE clause with metadata -> identify possible primary frags & derived frags

        // --> attribute AGE, where a fragmentation exists, is a selection condition of the query
        stmt = conn.createStatement();
        String queryFrag = "SELECT ID,MINVALUE,MAXVALUE FROM FRAGMETA WHERE TABLE='TA' AND ATTRIBUTE='AGE'";
        ResultSet res = stmt.executeQuery(queryFrag);
        while (res.next()) {
            System.out.println("Fragment (AGE) found: ID=" + res.getInt(1) + ", MIN="
                    + res.getInt(2) + ", MAX=" + res.getInt(3));
        }

        String queryCo = "SELECT ID, JOINATTR, COJOIN FROM COMETA WHERE TABLE='TB' AND COTABLE='TA'";
        res = stmt.executeQuery(queryCo);
        while (res.next()) {
            System.out.println("Copartition for TB to TA found: ID=" + res.getInt(1) + ", JOIN="
                    + res.getString(2) + ", COJOIN=" + res.getString(3));
        }



    }



}
