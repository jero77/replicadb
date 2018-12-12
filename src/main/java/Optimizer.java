import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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


//        // Metadata & initial fragments
//        DBMetadata meta = new DBMetadata(conn.getMetaData());
//        meta.addTableFragment("TA", "AGE", 0, 20);
//        meta.addTableFragment("TA", "AGE", 21, 40);
//        meta.addTableFragment("TA", "AGE", 41, 60);
//        meta.addTableFragment("TA", "AGE", 61, 80);
//        meta.addTableFragment("TA", "AGE", 81, 100);
//
//
//        // Query ...
//        String query = "SELECT * FROM TA, TB WHERE TA.AGE <= 35 AND TA.ID = TB.IDOFTA";
//
//        // ... parsing ...
//
//        List<TableFragment> list = meta.findMatchingFragment("TA", "AGE", "<=",35);
//        System.out.println("Fragments for selection condition TA.AGE <= 35");
//        if (list == null)
//            throw new IllegalArgumentException("No matching fragments found for table TA on selection TA.AGE <= 35");
//        else if (list.size() == 1) {
//            // only 1 fragment found --> it contains all the tuples we want
//            System.out.println(list.get(0).getTableName() + ".F" + list.get(0).getFragmentNumber() + " age "
//                    + list.get(0).getMinvalue() + "-" + list.get(0).getMaxvalue());
//            // ... query server hosting this fragment ...
//
//        } else if (list.size() > 1) {
//            // multiple fragments found
//            for (TableFragment frag : list)
//                System.out.println(frag.getTableName() + ".F" + frag.getFragmentNumber() + " age "
//                        + frag.getMinvalue() + "-" + frag.getMaxvalue());
//
//            // ... query all servers hosting those fragments ...
//            // ... collect tuples on one server ...
//        }
//
//
//        // Query ...
//        query = "SELECT * FROM TA, TB WHERE TA.AGE >= 21 AND TA.AGE < 45";
//
//        // ... parsing ...
//        list = meta.findMatchingFragment("TA", "AGE", 21, 45);
//        System.out.println("Fragments for selection condition TA.AGE >= 21 && TA.AGE <= 45");
//        for (TableFragment frag : list)
//            System.out.println(frag.getTableName() + ".F" + frag.getFragmentNumber() + " age "
//                    + frag.getMinvalue() + "-" + frag.getMaxvalue());
//


        // Store some meta-info
        PreparedStatement prep = conn.prepareStatement("INSERT INTO FRAGMETA (ID, TABLE, ATTRIBUTE, MINVALUE, " +
                "MAXVALUE) VALUES (?, ?, ?, ?, ?)");

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
        prep = conn.prepareStatement("INSERT INTO COMETA (ID, TABLE, JOINATTR, COTABLE, COJOIN) VALUES (?,?,?,?,?)");
        prep.setInt(1, 0);
        prep.setString(2, "TB");
        prep.setString(3, "IDOFTA");
        prep.setString(4, "TA");
        prep.setString(5, "ID");
        prep.executeUpdate();



        // Query ...
        String query = "SELECT * FROM TA, TB WHERE TA.AGE <= 35 AND TA.ID = TB.IDOFTA";

        // ... parsing ...
        // --> attribute AGE, where a fragmentation exists, is a selection condition of the query
        Statement stmt = conn.createStatement();
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


        // Query/Join can be processed!
        // stmt.executeQuery(query);

        // In case of no copartitioning of TB to TA


    }



}
