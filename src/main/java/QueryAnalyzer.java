import com.facebook.presto.sql.TreePrinter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import javafx.util.Pair;

import java.awt.*;
import java.sql.*;
import java.sql.Statement;
import java.util.*;
import java.util.List;

import static com.facebook.presto.sql.tree.ComparisonExpression.*;

/**
 * This class provides the analysis for a query. It extends the class {@link DefaultExpressionTraversalVisitor} to
 * traverse the given SQL-Query provided method {@link QueryAnalyzer#analyzePrint(String)} which prints out results
 * that occur during analysis of the query. Furthermore, this class enables the combination of metadata and information
 * of the query analysis to find matching fragmentations, create new fragmentations, ...
 */
public class QueryAnalyzer extends DefaultExpressionTraversalVisitor<Void, Void> {

    /**
     * Stores comparison expressions for further analysis
     */
    private ArrayList<ComparisonExpression> comparisons;

    /**
     * Stores the joins in the analyzed query found in WHERE and FROM clause
     */
    private ArrayList<Join> joins;

    /**
     * Stores possible fragment candidates to analyze if there are fragmentations that can match that selections
     */
    private ArrayList<ComparisonExpression> fragCandidates;

    /**
     * Saves the last analyzed SQL query
     */
    private String lastAnalyzedSql;

    /**
     * Connection to DB
     */
    private Connection conn;

//########################### Constructors ###################################

    /**
     * Init
     * @param conn Connection to the database
     */
    public QueryAnalyzer(Connection conn) {
        super();
        comparisons = new ArrayList<ComparisonExpression>();
        joins = new ArrayList<Join>();
        fragCandidates = new ArrayList<ComparisonExpression>();
        lastAnalyzedSql = null;

        this.conn = conn;
    }


//########################### Overwritten Methods ###################################

    /**
     * When visiting a {@link ComparisonExpression}, the expression is stored into the HashMap for analysis
     *
     * @param node
     * @param context
     * @return
     */
    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, Void context) {

        // Store in hashmap to analyze after where condition traversal
        comparisons.add(node);

        return super.visitComparisonExpression(node, context);
    }

    @Override
    protected Void visitDereferenceExpression(DereferenceExpression node, Void context) {
        return super.visitDereferenceExpression(node, context);
    }

    @Override
    protected Void visitValues(Values node, Void context) {
        return super.visitValues(node, context);
    }

//########################### Getter & Setter ###################################

    public ArrayList<ComparisonExpression> getComparisons() {
        return comparisons;
    }

    public String getLastAnalyzedSql() {
        return lastAnalyzedSql;
    }

    public ArrayList<Join> getJoins() {
        return joins;
    }

    public ArrayList<ComparisonExpression> getFragCandidates() {
        return fragCandidates;
    }


//########################### Analysis Methods ###################################

    /**
     * Analyze a given SQL query and print the results
     *
     * @param sql SQL query
     * @return True, if the query could be analyzed; false otherwise
     */
    public boolean analyzePrint(String sql) {

        Expression e;

        // update class variables
        comparisons.clear();
        fragCandidates.clear();
        joins.clear();
        lastAnalyzedSql = sql;

        // parsing the expression with presto-parser to obtain the query body
        SqlParser parser = new SqlParser();
        Query query = (Query) parser.createStatement(sql, new ParsingOptions());
        QueryBody queryBody = query.getQueryBody();


        // Analyze FROM expression (should be present, otherwise throw an exception)
        if (analyzeFromExpression(queryBody)) {
            System.out.println("FROM expression was successfully analyzed!");
        } else {
            // false means no join is present so only select from one table (no co-partitioning)
            System.out.println("FROM expression was be analyzed but no join was found!");

        }


        // Analyze WHERE expression (if present)
        if (analyzeWhereExpression(queryBody)) {
            System.out.println("WHERE expression was successfully analyzed!");

            // process possible fragment candidates obtained from selection conditions from WHERE analysis
            if (! fragCandidates.isEmpty()) {
                System.out.println("Testing " + fragCandidates.size() + " fragment candidates ... ");       // DEBUG
                ListMultimap<String, Integer> frags = testFragCandidates();
                System.out.println(frags.size() + " fragments found: ");   // DEBUG

                if (frags.isEmpty()) {
                    // There are no fragments based on selection conditions
                    for (ComparisonExpression comp : fragCandidates) {
                        makeFragmentationForComparisonExpression(comp);
                    }
                }
                else
                    for (String key : frags.keySet()) {
                        // Process the found fragments
                        System.out.println("\t -> on " + key + " fragmentIDs="
                                + Arrays.toString(frags.get(key).toArray()));       // DEBUG
                        // TODO process the fragments --> find server? rewrite query? redirect query to server?
                        // TODO what if there are multiple selection conditions with multiple fragmentations?
                    }

            } else {
                System.out.println("Found no fragment candidates for the given query (for selection conditions) ... ");
                // TODO query all servers and collect tuples on one server? store tuples to "collection" server or just answer the query?
            }

        } else {
            System.out.println("WHERE expression could not be analyzed! An error occured or maybe there is no WHERE...");
        }



        // Process all joins found in the WHERE and FROM analysis
        if (! joins.isEmpty()) {
            System.out.println("Found " + joins.size() + " joins. Testing joins for co-partitions ...");
            HashMap<Join, Integer> copartitions = testJoinsForCopartitions();
            for (Join j : copartitions.keySet()) {
                System.out.println("\t -> Join: " + j + ", copartitionID: " + copartitions.get(j));
            }
        } else {
            System.out.println("Found no joins in WHERE and FROM clause ...");
            // TODO So only selection from one table? Or something else?

        }

        return true;
    }


    /**
     * Process the where expression from the given {@link QuerySpecification}
     * @param queryBody The body of a query
     * @return True if the WHERE expression in the body of the query could be analyzed; false otherwise
     */
    private boolean analyzeWhereExpression(QueryBody queryBody) {

        if (! (queryBody instanceof QuerySpecification))
            throw new IllegalArgumentException("The argument queryBody of the method analyzeWhereExpression in class " +
                    this.getClass().getName() + " is not an instance of the class "
                    + QuerySpecification.class.getName() + "!");

        // Try to get WHERE expression
        Expression e;
        Optional<Expression> optWhere = ((QuerySpecification) queryBody).getWhere();
        if (! optWhere.isPresent()) {
            System.out.println("Found no WHERE clause in the query!");
            return false;
        }
        e = optWhere.get();
        System.out.println("Where-Expression: " + e);

        // Tree debug
        //printAstTree(e);

        // traversal of the expression with the process method of superclass to classify subexpressions
        // --> this fills amongst other things the list of comparisons (for further processing)
        super.process(e);


        // Analyze the found comparisons & save information to process the joins in WHERE clause for co-partitioning
        // or to process selection conditions on (possibly) fragmented attributes to optimize the query
        System.out.println("Found the following comparisons in the WHERE expression: " + e);
        for (ComparisonExpression comp : comparisons) {
            System.out.print(comp + ", processing comparison ... ");
            processComparisonExpression(comp);
        }

        return true;
    }


    /**
     * Process the given comparison:
     *  - Identify joins of two tables, e.g. T.a = S.b
     *  - Identify attribute-value-comparisons, e.g. T.a = 5 or S.b <= 14.5677
     * @param comp
     */
    private void processComparisonExpression(ComparisonExpression comp) {

        // Disassemble comparison
        Expression left, right;
        left = comp.getLeft();
        right = comp.getRight();
        Operator op = comp.getOperator();

        if (left instanceof DereferenceExpression && right instanceof DereferenceExpression) {

            DereferenceExpression dref_left, dref_right;
            dref_left = (DereferenceExpression) left;
            dref_right = (DereferenceExpression) right;

            if (op.equals(Operator.EQUAL)) {         // IMPLICIT JOIN of left and right DereferenceExpressions
                System.out.println("Found an IMPLICIT JOIN! (will be stored as INNER JOIN because an IMPLICIT JOIN " +
                        "cannot have JoinCriteria ...)");

                // Get columns & create JoinCriteria (maybe not needed ...)
                List<Identifier> columns = new ArrayList<Identifier>();
                columns.add(dref_left.getField());
                columns.add(dref_right.getField());
                JoinCriteria joinCriteria = new JoinUsing(columns);  // TODO maybe change column list creation

                // Store join
                joins.add(new Join(Join.Type.INNER, new Table(QualifiedName.of(dref_left.getBase().toString())),
                        new Table(QualifiedName.of(dref_right.getBase().toString())), Optional.of(joinCriteria)));
            } else {
                // Operator is of some other type: <, <=, >=, >, <>, ...
            }

        } else if (left instanceof DereferenceExpression) {     // Some comparison of <attr> <op> <val>???
            System.out.println("Found a value comparison (left)");
            // Store it to analyze for matching fragmentations of that table
            fragCandidates.add(comp);

        } else if (right instanceof DereferenceExpression) {    // Some comparison of <val> <op> <attr>???
            System.out.println("Found a value comparison (right)");
            // Flip the comparison & store it to analyze for matching fragmentations of that table
            fragCandidates.add( new ComparisonExpression(op.flip(), right, left));

        } else {
            System.err.println("Don't know how to process the ComparisonExpression: " + comp);
        }

    }


    /**
     * This method analyzes the FROM expression in the given query body whether it contains a join of tables or not. It
     * will have a look at the JOIN of (two or more) tables (if present) and will store joins for later analysis in
     * a list.
     * @param queryBody
     * @return True if the FROM expression was successfully analyzed; false if there is no join
     */
    private boolean analyzeFromExpression(QueryBody queryBody) {

        if (! (queryBody instanceof QuerySpecification))
            throw new IllegalArgumentException("The argument queryBody of the method analyzeFromExpression in class " +
                    this.getClass().getName() + " is not an instance of the class "
                    + QuerySpecification.class.getName() + "!");

        // Try to get FROM
        QuerySpecification specification = (QuerySpecification) queryBody;
        Optional<Relation> optFrom = specification.getFrom();
        if (! optFrom.isPresent())
            throw new IllegalArgumentException("No FROM expression found in the query body: " + queryBody);

        // Try to get join(s), if not present there is no join of two or more tables --> no need for co-partitioning
        Relation relation = optFrom.get();
        if (! (relation instanceof Join)) {
            System.out.println("The FROM expression does not contain a join (at least the relation " + relation
                    + " is not an instance of " + Join.class.getName() + " but of " + relation.getClass().getName());
            return false;
        }
        Join join = (Join) relation;
        System.out.println("Found a join: " + join);

        if (join.getType().equals(Join.Type.IMPLICIT)) {
            // The join is implicit => cf. WHERE expression analysis, return true because FROM analysis is done
            return true;
        } else {

            Optional<JoinCriteria> optCriteria = join.getCriteria();
            if (optCriteria.isPresent()) {
                if (optCriteria.get() instanceof JoinOn) {
                    // Disassemble expression e (--> ComparisonExpression with DerefExpr. = DerefExpr.) & process it
                    // This transforms the JoinOn to a JoinUsing with column list
                    JoinOn joinOn = (JoinOn) optCriteria.get();
                    Expression e = joinOn.getExpression();
                    System.out.println("JoinOn: " + joinOn + ", expr: " + e);        // DEBUG

                    if (! (e instanceof ComparisonExpression))
                        throw new IllegalArgumentException("The expression " + e + " of the JoinOn " + joinOn + " " +
                                "from the join " + join + " is not a ComparisonExpression, but an instance of class "
                                + e.getClass().getName());

                    processComparisonExpression((ComparisonExpression) e);
                } else {
                    joins.add(join);    // add it to the list of joins
                }
            }

            // TODO what if criteria not present
        }

        // TODO what is with 3+ tables being joined in FROM? Is the structure TA Join (TB Join TC) or how else?

        return true;
    }



// ########################### Metadata information ################################


    /**
     * This method tests the fragment candidates identified for the analyzed query to obtain the relevant fragment ids.
     * @return All fragments that need to be accessed when executing the query
     */
    private ListMultimap<String, Integer> testFragCandidates() {

        // Match information about fragmentations (FRAGMENTA) with the query attribute-value-comparisons
        // This multimap stores for each attribute name all attribute-value-comparison
        ListMultimap<String, ComparisonExpression> attrComps = MultimapBuilder.treeKeys().arrayListValues().build();

        // This multimap stores for each attribute all fragment ids that match the comparisons
        ListMultimap<String, Integer> result = MultimapBuilder.treeKeys().arrayListValues().build();

        // Store all comparisons into the multimap with their attribute reference (table.name) as key
        for (ComparisonExpression comp : fragCandidates) {
            DereferenceExpression left = (DereferenceExpression) comp.getLeft();
            attrComps.put(DereferenceExpression.getQualifiedName(left).toString().toUpperCase(), comp);
        }

        // For every attribute test if it is compared once or several times
        for (String attribute : attrComps.keySet()) {

            List<ComparisonExpression> list = attrComps.get(attribute);
            if (list.size() == 1) {     // only once compared

                // Get all the fragments matching this comparison
                result.putAll(attribute, getFragsOfComparison(list.get(0)));

            } else if (list.size() == 2) {      // twice compared, e.g. T.a >= 15 AND T.a <= 20

                // get the comparisons
                ComparisonExpression a, b;
                a = list.get(0);
                b = list.get(1);
                result.putAll(attribute, getFragsOfComparisons(a, b));


            } else throw new IllegalArgumentException("Cannot process the comparisons for the attribute " + attribute
                    + " because there are too many comparisons on this attribute. Either the conditions are redundant"
                    + " or the query might fail anyways due to contradictory selection conditions!");

        }


        // return the found assignment for the comparisons
        return result;

    }


    /**
     * This method checks if a comparison to a value (e.g. T.AGE >= 5) matches a given range.
     * NOTE: Comparison with "IS DISTINCT FROM' results in false always
     * @param op Comparison operator
     * @param compvalue Comparison value
     * @param minvalue Range minimal value
     * @param maxvalue Range maximal
     * @return Result of the check
     */
    private boolean matchCompAndFragMeta(Operator op, long compvalue, long minvalue, long maxvalue) {
        switch (op) {
            case EQUAL:
                return (minvalue <= compvalue && compvalue <= maxvalue);
            case NOT_EQUAL:
                return (minvalue > compvalue && compvalue > maxvalue);
            case LESS_THAN:
                return (minvalue < compvalue);
            case LESS_THAN_OR_EQUAL:
                return (minvalue <= compvalue);
            case GREATER_THAN:
                return (maxvalue > compvalue);
            case GREATER_THAN_OR_EQUAL:
                return (maxvalue >= compvalue);
            case IS_DISTINCT_FROM:
        }
        return false;
    }


    /**
     * This method gets the fragments matching the selection condition in form of two comparisons on the same attribute
     * @param a ComparisonExpression of the form T.x op value
     * @param b ComparisonExpression of the form T.x op2 value2
     * @return Returns a list which contains the fragment ids
     */
    private ArrayList<Integer> getFragsOfComparisons(ComparisonExpression a, ComparisonExpression b) {

        ArrayList<Integer> result = new ArrayList<Integer>();

        // The resulting list of fragments is the intersection of the two fragment lists of both comparisons alone
        ArrayList<Integer> afrags = getFragsOfComparison(a);
        ArrayList<Integer> bfrags = getFragsOfComparison(b);
        for (Integer aid : afrags) {
            if (bfrags.contains(aid))
                result.add(aid);
        }

        // If the intersection of both is empty, the two comparisons are unsatisfiable together (e.g. x>5 && x<0)
        if (result.isEmpty())
            throw new IllegalArgumentException("The intersection of the two fragment lists of comparisons " + a
                    + " and " + b + " is empty because the selection conditions are unsatisfiable which makes the" +
                    " query unsatisfiable!");
        return result;
    }


    /**
     * This method returns all fragments matching the given comparison
     * @param comp The comparison
     * @return
     */
    private ArrayList<Integer> getFragsOfComparison(ComparisonExpression comp) {

        ArrayList<Integer> result = new ArrayList<Integer>();

        // Get the tablename and attributename
        String tablename, attrname;
        String attributeref =
                DereferenceExpression.getQualifiedName((DereferenceExpression) comp.getLeft()).toString().toUpperCase();
        tablename = attributeref.split("\\.")[0];      // attributeref = <table>.<attribute> (hopefully)
        attrname = attributeref.split("\\.")[1];

        // get operator and value
        Operator op = comp.getOperator();
        Expression right = comp.getRight();
        long compvalue = 0;
        if (right instanceof LongLiteral) {
            LongLiteral longLit = (LongLiteral) right;
            compvalue = longLit.getValue();
        } else throw new IllegalArgumentException("The value of the comparison is not a long but a "
                + right.getClass().getName());


        // Query the fragmentation meta data table with the table and attribute name
        String sql = "SELECT ID,MINVALUE,MAXVALUE FROM FRAGMETA WHERE TABLE=? AND ATTRIBUTE=?";
        PreparedStatement prep;
        try {
            prep = conn.prepareStatement(sql);
            prep.setString(1, tablename);
            prep.setString(2, attrname);
            ResultSet res = prep.executeQuery();

            // If a fragment range matches, then put it into the result
            while (res.next()) {
                int fragmentID = res.getInt(1);
                if (matchCompAndFragMeta(op, compvalue, res.getInt(2), res.getInt(3))) {
                    result.add(fragmentID);
                }
            }
        } catch (SQLException e) {
            System.err.println("ERROR for PreparedStatement to query FRAGMETA in " + this.getClass().getName());
            e.printStackTrace();
        }

        return result;
    }


    /**
     * Test the joins from the WHERE clause, e.g. WHERE ... AND T.a = S.b AND ..., whether the joined tables are
     * co-partitioned on that attribute(s); if a co-partitioning is found, then the join is added with the id of the
     * metadata tuple to the result HashMap
     * @return Maps Joins to co-partition metadata tuple ids; null if no matching metadata tuple was found (i.e. the
     *          joined tables are not co-partitioned)
     */
    private HashMap<Join, Integer> testJoinsForCopartitions() {

        HashMap<Join, Integer> result = new HashMap<Join, Integer>();

        for (Join join : joins) {

            // Get the tablenames and attribute names (join should have tables t1.a1 and t2.a2 if only join of 2 tables)
            Table left, right;
            left = (Table) join.getLeft();
            right = (Table) join.getRight();
            String table, cotable, joinattr, cojoin;
            // Table names
            table = left.getName().toString().toUpperCase();
            cotable = right.getName().toString().toUpperCase();
            // Attribute names
            JoinUsing criteria = (JoinUsing) join.getCriteria().get();
            List<Identifier> joinColumns = criteria.getColumns();
            joinattr = joinColumns.get(0).getValue();
            cojoin = joinColumns.get(1).getValue();
            // TODO maybe check size --> 3+ tables joined 3+ columns in list?!
            // TODO maybe assign attribute names not according to position in arraylist but according to tablename?

            // Query the co-partitioning meta data table with the table and attribute names
            String sql = "SELECT ID FROM COMETA WHERE TABLE=? AND JOINATTR = ? AND COTABLE=? AND COJOIN=?";
            PreparedStatement prep;
            try {
                prep = conn.prepareStatement(sql);
                prep.setString(1, table);
                prep.setString(2, joinattr);
                prep.setString(3, cotable);
                prep.setString(4, cojoin);
                ResultSet res = prep.executeQuery();

                // If a co-partitioning is found, then store join & the co-partitioning id
                if (res.next()) {
                    int copartID = res.getInt(1);
                    result.put(join, copartID);
                    continue;
                }

                // Otherwise, check the other direction (join is bidirectional); and if nothing is found here, there is
                // no co-partitioning and so store the join together with a null value to the result
                prep.setString(1, cotable);
                prep.setString(2, cojoin);
                prep.setString(3, table);
                prep.setString(4, joinattr);
                res = prep.executeQuery();
                if (res.next()) {
                    int copartID = res.getInt(1);
                    result.put(join, copartID);
                } else {
                    result.put(join, null);
                }

            } catch (SQLException e) {
                System.err.println("ERROR for PreparedStatement to query FRAGMETA in " + this.getClass().getName());
                e.printStackTrace();
            }
        }

        return result;
    }


    /**
     * Generates the ordered list with all column names and their SQL-Types from a certain table in the following form:
     *      List = ["Column1;TypeA", "Column2;TypeB", ...]
     * @param table Table name
     * @return Column List
     */
    private ArrayList<String> getColumnsFromTable(String table) {

        ArrayList<String> columns = new ArrayList<String>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("SELECT * FROM " + table);     // Dummy query to get metadata
            ResultSetMetaData meta = res.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i) + ";" + meta.getColumnTypeName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columns;
    }


    /**
     * This method returns a list of all primary column names of a certain table
     * @param table Table name
     * @return Primary key columns
     */
    private ArrayList<String> getPrimaryKeysFromTable(String table) {
        ArrayList<String> primaryKeys = new ArrayList<String>();
        try {
            ResultSet res = conn.getMetaData().getPrimaryKeys("", "", table);
            while (res.next()) {
                primaryKeys.add(res.getString("PK_NAME"));      // Should be "COLUMN_NAME"? maybe Ignite ...
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return primaryKeys;
    }




// ########################## Fragmentation Management ##############################################


    /**
     * This method will create a new fragmentation for the given comparison expression (which is a selection condition
     * in the currently analyzed query). The ComparisonExpression has to be of the form '<DereferenceExpression> <op>
     * <Value>'. For the sake of simplicity, the newly created fragmentation will consist of two selection conditions:
     * the one given as argument to this function and the negated version of it (other direction).
     * After this, the method will rearrange the data to match the fragmentation and update the metadata.
     * It is assumed that there is no fragmentation present for the given table attribute combination
     * @param comp
     */
    private void makeFragmentationForComparisonExpression(ComparisonExpression comp) {

        // Decompose ComparisonExpression
        DereferenceExpression left = (DereferenceExpression) comp.getLeft();
        String table = left.getBase().toString().toUpperCase();
        String attribute = left.getField().getValue().toUpperCase();
        Expression right = comp.getRight();
        int value = (int) ((LongLiteral) right).getValue();
        Operator op = comp.getOperator();
        Operator negop = op.negate();

        // Update the metadata & create string for fragment table
        Integer[] minMax = setMinMaxValue(op, value);
        int fragID = metaMakeNewFragment(table, attribute, minMax[0], minMax[1]);
        String create = buildFragmentCreateString(table, fragID);

        // Update metadata for the second negated fragment & create string
        minMax = setMinMaxValue(negop, value);
        fragID = metaMakeNewFragment(table, attribute, minMax[0], minMax[1]);
        String negCreate = buildFragmentCreateString(table, fragID);

        // Create the two fragment tables
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(create);
            stmt.execute(negCreate);
        } catch (SQLException e) {
            e.printStackTrace();
        }


        // TODO rearrangement
        // Rearrange the data according to the new fragmentation: insert data into the new fragments from all 'old'
        // fragments matching the selection condition of the fragment
        String insert;
        try {
            // Insert for two new fragments
            insert = buildFragmentInsertString(table, fragID, attribute, op, value);
            PreparedStatement prep = conn.prepareStatement(insert);
//            prep.executeUpdate();
            insert = buildFragmentInsertString(table, fragID, attribute, op, value);
//            prep = conn.prepareStatement(insert);
//            prep.executeUpdate();
            //todo where to execute queries?
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }


    }


    /**
     * Update the metadata and set a new fragment according to the arguments. If minvalue is null, it means that for all
     * tuples in the fragment holds that attribute <= maxvalue, and if maxvalue is null, for all tuples in the fragment
     * holds that attribute >= minvalue. If both are null, this is an error!
     * @param table Name of the table
     * @param attribute Name of the attribute
     * @param minvalue Minimum value of the fragment (inclusively)
     * @param maxvalue Maximum value of the fragment (inclusively)
     * @return The ID of the fragment (name is 'table_ID'), -1 on error
     */
    private int metaMakeNewFragment(String table, String attribute, Integer minvalue, Integer maxvalue) {

        if (minvalue == null && maxvalue == null)
            throw new IllegalArgumentException("The both arguments minvalue and maxvalue must not be null at the same" +
                    " time!");


        // Find the next ID to be used to store a fragment of this table
        int nextID = 0;
        try {
            PreparedStatement prep = conn.prepareStatement("SELECT MAX(ID) FROM FRAGMETA WHERE TABLE = ?");
            prep.setString(1, table);
            ResultSet res = prep.executeQuery();
            if (res.next())
                nextID = res.getInt(1) + 1;


            // Store the fragment in the metadata table (with NULL value for minvalue or maxvalue if needed)
            prep = conn.prepareStatement("INSERT INTO FRAGMETA (ID, TABLE, ATTRIBUTE, MINVALUE, MAXVALUE) VALUES " +
                    "(?,?,?,?,?)");
            prep.setInt(1, nextID);
            prep.setString(2, table);
            prep.setString(3, attribute);
            if (minvalue == null)
                prep.setNull(4, Types.INTEGER);
            else
                prep.setInt(4, minvalue);
            if (maxvalue == null)
                prep.setNull(5, Types.INTEGER);
            else
                prep.setInt(5, maxvalue);

            // Execute update
            int rowsupdated = prep.executeUpdate();
            if (rowsupdated != 1)
                nextID = -1;    //todo exception?
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return nextID;

    }


    /**
     * This method computes the minvalue and maxvalue to a given operator and a value.
     * @param op Operator
     * @param value Value
     * @return Integer[] = {minvalue, maxvalue}
     */
    private Integer[] setMinMaxValue(Operator op, Integer value) {
        Integer minvalue = null;
        Integer maxvalue = null;
        switch (op) {
            case LESS_THAN:
                maxvalue = value - 1;
                break;
            case LESS_THAN_OR_EQUAL:
                maxvalue = value;
                break;
            case GREATER_THAN:
                minvalue = value + 1;
                break;
            case GREATER_THAN_OR_EQUAL:
                maxvalue = value;
                break;
            default :
                throw new IllegalArgumentException("The given setMinMaxValue-Operation is not valid because it " +
                        "contains an operator which is not <, <=, >, >=!");
        }
        Integer[] res = {minvalue, maxvalue};
        return res;
    }



    /**
     * This method builds the CREATE TABLE SQL statement which will be used for creation of a fragment of a given table
     * @param table Table name
     * @param fragID ID of the fragment (used in name of the fragment table)
     * @return SQL CREATE TABLE statement
     */
    private String buildFragmentCreateString(String table, int fragID) {

        // Get the columns and primary keys
        ArrayList<String> columns, primaryKeys;
        columns = getColumnsFromTable(table);
        primaryKeys = getPrimaryKeysFromTable(table);

        // Add the column list to the build
        StringBuilder create = new StringBuilder("CREATE TABLE " + table + "_" + fragID + " (");
        for (String s : columns) {
            create.append(s.replace(";", " ") + ", ");
        }

        // Add the primary key to the build
        create.append("PRIMARY KEY (");
        if (primaryKeys.size() < 1) {
            // todo exception? or what?
        }
        create.append(primaryKeys.remove(0));
        for (String s : primaryKeys) {
            create.append(", " + s);
        }
        create.append(") )");

        // return the build
        return create.toString();
    }


    /**
     * Builds an INSERT INTO SQL statement for a given table fragment
     * @param table Table name
     * @param fragID ID of the fragment (used in table name)
     * @param attribute Attribute name
     * @param op Operator in selection condition
     * @param value int value in selection condition
     * @return SQL INSERT statement
     */
    private String buildFragmentInsertString(String table, int fragID, String attribute, Operator op, int value) {

        // Start to build
        StringBuilder insert = new StringBuilder("INSERT INTO " + table + "_" + fragID + " (");

        // Add column names of the table to the build
        ArrayList<String> columns = getColumnsFromTable(table);
        if (columns.size() < 1)
            ;   // todo what to do? exception?
        insert.append(columns.remove(0).split(";")[0]);
        for (String s : columns) {
            insert.append("," + s.split(";")[0]);       // only name needed; type irrelevant here
        }
        insert.append(") ");

        // Add subquery with selection condition for fragment
        insert.append("(SELECT * FROM " + table + " WHERE " + attribute + op.getValue() + value + ")");

        // return build
        System.out.println(insert.toString());      // DEBUG
        return insert.toString();
    }


// ################################ DEBUG Stuff ###########################################

    /**
     * Prints the AST-Tree to the given expression to {@link System#out}
     * @param e
     */
    private void printAstTree(Expression e) {
        IdentityHashMap<Expression, QualifiedName> ihm = new IdentityHashMap<Expression, QualifiedName>();
        ihm.put(e, QualifiedName.of(e.toString()));
        TreePrinter tp = new TreePrinter(ihm, System.out);
        tp.print(e);
    }


}
