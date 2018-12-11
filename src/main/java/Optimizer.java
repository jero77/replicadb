import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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


        // Metadata & initial fragments
        DBMetadata meta = new DBMetadata(conn.getMetaData());
        meta.addTableFragment("TA", "AGE", 0, 20);
        meta.addTableFragment("TA", "AGE", 21, 40);
        meta.addTableFragment("TA", "AGE", 41, 60);
        meta.addTableFragment("TA", "AGE", 61, 80);
        meta.addTableFragment("TA", "AGE", 81, 100);


        // Query ...
        String query = "SELECT * FROM TA, TB WHERE TA.AGE <= 35 AND TA.ID = TB.IDOFTA";

        // ... parsing ...

        List<TableFragment> list = meta.findMatchingFragment("TA", "AGE", "<=",35);
        System.out.println("Fragments for selection condition TA.AGE <= 35");
        if (list == null)
            throw new IllegalArgumentException("No matching fragments found for table TA on selection TA.AGE <= 35");
        else if (list.size() == 1) {
            // only 1 fragment found --> it contains all the tuples we want
            System.out.println(list.get(0).getTableName() + ".F" + list.get(0).getFragmentNumber() + " age "
                    + list.get(0).getMinvalue() + "-" + list.get(0).getMaxvalue());
            // ... query server hosting this fragment ...

        } else if (list.size() > 1) {
            // multiple fragments found
            for (TableFragment frag : list)
                System.out.println(frag.getTableName() + ".F" + frag.getFragmentNumber() + " age "
                        + frag.getMinvalue() + "-" + frag.getMaxvalue());

            // ... query all servers hosting those fragments ...
            // ... collect tuples on one server ...
        }


        // Query ...
        query = "SELECT * FROM TA, TB WHERE TA.AGE >= 21 AND TA.AGE < 45";

        // ... parsing ...
        list = meta.findMatchingFragment("TA", "AGE", 21, 45);
        System.out.println("Fragments for selection condition TA.AGE >= 21 && TA.AGE <= 45");
        for (TableFragment frag : list)
            System.out.println(frag.getTableName() + ".F" + frag.getFragmentNumber() + " age "
                    + frag.getMinvalue() + "-" + frag.getMaxvalue());
    }

}
