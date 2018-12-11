import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DBMetadata {


    private HashSet<String> tableNames;

    private HashMap<String, ArrayList<TableFragment>> tableFragments;


    private DatabaseMetaData metadata;


    public DBMetadata(DatabaseMetaData metadata) {
        this.metadata = metadata;

        // Get table names
        this.tableNames = new HashSet<String>();
        try {
            this.tableNames.addAll(this.getTablesMetadata());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // init table fragment lists
        tableFragments = new HashMap<String, ArrayList<TableFragment>>();
        for (String name : tableNames) {
            ArrayList<TableFragment> list = new ArrayList<TableFragment>();
            tableFragments.put(name, list);
        }
    }


    /**
     * @return Arraylist with the table's name
     * @throws SQLException
     */
    public ArrayList getTablesMetadata() throws SQLException {
        String table[] = {"TABLE"};
        ResultSet rs = null;
        ArrayList tables = null;


        rs = metadata.getTables(null, null, null, table);
        tables = new ArrayList();
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }


    /**
     * Add a new fragment for a table with fragmentation attribute and minimal and maximal value for this attribute.
     *
     * @param table
     * @param fragmentationAttribute
     * @param minvalue
     * @param maxvalue
     */
    public void addTableFragment(String table, String fragmentationAttribute, int minvalue, int maxvalue) {
        ArrayList<TableFragment> list = tableFragments.get(table);
        int nextFragNo = tableFragments.get(table).size();
        TableFragment frag = new TableFragment(table, fragmentationAttribute, minvalue, maxvalue, nextFragNo);
        list.add(frag);
    }


    /**
     * Get a list of all fragments of a given table
     *
     * @param table
     * @return
     */
    public List<TableFragment> getAllTableFragments(String table) {
        if (tableNames.contains(table))
            return tableFragments.get(table);
        return null;
    }


    /**
     * Get a list of all fragments over a certain attribute of a given table
     *
     * @param table
     * @param attribute
     * @return
     */
    public List<TableFragment> getTableFragments(String table, String attribute) {
        List<TableFragment> allFrags = getAllTableFragments(table);
        if (allFrags == null) {
            return null;
        }

        for (TableFragment frag : allFrags) {
            if (frag.getFragmentationAttribute().equals(attribute))
                ;
            else
                allFrags.remove(frag);
        }
        return allFrags;
    }


    /**
     * Find the fragments that match a certain selection condition on a given table, e.g. age >= 10.
     *
     * @return
     */
    public List<TableFragment> findMatchingFragment(String table, String attribute, String compare, int value) {

        List<TableFragment> res = new ArrayList<TableFragment>();

        if (compare.equals(">="))
            return this.findMatchingFragment(table, attribute, value, Integer.MAX_VALUE);
        else if (compare.equals(">"))
            return this.findMatchingFragment(table, attribute, value + 1, Integer.MAX_VALUE);
        else if (compare.equals("<="))
            return this.findMatchingFragment(table, attribute, Integer.MIN_VALUE, value);
        else if (compare.equals("<"))
            return this.findMatchingFragment(table, attribute, Integer.MIN_VALUE, value - 1);
        else if (compare.equals("="))
            return this.findMatchingFragment(table, attribute, value, value);

        return null;
    }


    /**
     * Find the fragments that match a certain selection condition on a given table with minimal and maximal value,
     * e.g. age <= 50 and age >= 20.
     *
     * @param table
     * @param attribute
     * @param minval
     * @param maxval
     * @return
     */
    public List<TableFragment> findMatchingFragment(String table, String attribute, int minval, int maxval) {

        List<TableFragment> res = new ArrayList<TableFragment>();

        // Are there fragmentations on this attribute?
        List<TableFragment> candidates = this.getTableFragments(table, attribute);
        if (candidates == null)
            return null;

        // Collect matching fragments
        for (TableFragment frag : candidates) {
            int min = frag.getMinvalue();
            int max = frag.getMaxvalue();

            if ((minval >= min && minval <= max) || (maxval <= max && maxval >= min) ||
                    (minval <= min && maxval >= max)) {
                res.add(frag);
            }
        }

        return res;
    }


}
