import com.facebook.presto.sql.TreePrinter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import javafx.util.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.facebook.presto.sql.tree.ComparisonExpression.*;

/**
 * This class provides the analyzation for the selection condition of a query. It extends the class
 * {@link DefaultExpressionTraversalVisitor} to traverse the given SQL-Query containing a WHERE clause with the
 * provided method {@link SelectionConditionAnalyzer#analyzePrint(String)}
 */
public class SelectionConditionAnalyzer extends DefaultExpressionTraversalVisitor<Void, Void> {

    /**
     * Stores comparison expressions
     */
    private ArrayList<ComparisonExpression> comparisons;

    /**
     * Stores the joins in the analyzed query found in WHERE clause
     */
    private ArrayList<Join> joins;

    /**
     * Stores possible fragment candidates to analyze if there are fragmentations that can match that selections
     */
    private ArrayList<ComparisonExpression> fragCandidates;

    /**
     * Saves the last analyzed expression
     */
    private Expression lastAnalyzed;


    /**
     * Connection to DB
     */
    private Connection conn;

//########################### Constructors ###################################

    /**
     * Init
     * @param conn Connection to the database
     */
    public SelectionConditionAnalyzer(Connection conn) {
        super();
        comparisons = new ArrayList<ComparisonExpression>();
        joins = new ArrayList<Join>();
        fragCandidates = new ArrayList<ComparisonExpression>();
        lastAnalyzed = null;

        this.conn = conn;
    }


//########################### Overwritten Methods ###################################

    /**
     * When visiting a {@link ComparisonExpression}, the expression is stored into the HashMap for analyzation
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

    public Expression getLastAnalyzed() {
        return lastAnalyzed;
    }

    public ArrayList<Join> getJoins() {
        return joins;
    }

    public ArrayList<ComparisonExpression> getFragCandidates() {
        return fragCandidates;
    }


    //########################### Analyzing Methods ###################################

    /**
     * Analyze a given SQL-Query containing a conjunctive WHERE clause without subqueries and print the results
     *
     * @param sqlWithWhere SQL-Query with WHERE
     * @return True, if a WHERE clause was found and analyzed; false otherwise
     */
    public boolean analyzePrint(String sqlWithWhere) {

        Expression e;

        // parsing with presto-parser
        SqlParser parser = new SqlParser();
        Query query = (Query) parser.createStatement(sqlWithWhere, new ParsingOptions());
        Optional<Expression> optional = ((QuerySpecification) query.getQueryBody()).getWhere();
        if (optional.isPresent())
            e = optional.get();
        else {
            System.out.println("No WHERE clause contained");
            return false;
        }
        System.out.println("Where-Expression: " + e);

        // update class variables
        comparisons.clear();
        lastAnalyzed = e;

        // traversal of the expression
        process(e);
        // print AST tree
//        IdentityHashMap<Expression, QualifiedName> ihm = new IdentityHashMap<Expression, QualifiedName>();
//        ihm.put(e, QualifiedName.of(e.toString()));
//        TreePrinter tp = new TreePrinter(ihm, System.out);
//        tp.print(e);

        // Analyze the Comparisons
        System.out.println("Found the following comparisons in the expression: " + e);
        for (ComparisonExpression comp : comparisons) {
            System.out.print(comp + " ... ");
            processComparisonExpression(comp);
        }

        if (! fragCandidates.isEmpty()) {
            ListMultimap<String, Integer> frags = testFragCandidates();
            System.out.println(frags.size() + " possible fragment candidates found: ");

            for (String key : frags.keySet()) {
                System.out.println("Key: " + key + ", FragmentIDs: " + Arrays.toString(frags.get(key).toArray()));
            }
        } else {
            System.out.println("Found no fragmentation candidates ... ");
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

            if (op.equals(Operator.EQUAL)) {         // INNER JOIN of left and right DereferenceExpressions
                System.out.println("Found an INNER JOIN!");

                // Get columns & create JoinCriteria (maybe not needed ...)
                List<Identifier> columns = new ArrayList<Identifier>();
                columns.add(new Identifier(left.toString()));
                columns.add(new Identifier(right.toString()));
                JoinCriteria joinCriteria = new JoinUsing(columns);

                // Store join
                joins.add(new Join(Join.Type.INNER, new Table(QualifiedName.of(left.toString())),
                        new Table(QualifiedName.of(right.toString())), Optional.of(joinCriteria)));
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



    private ListMultimap<String, Integer> testFragCandidates() {

        // Match information about fragmentations (FRAGMENTA) with the query attribute-value-comparisons
        // Multimap assigns attribute-value comparison to fragments
        ListMultimap<String, Integer> frags = MultimapBuilder.treeKeys().arrayListValues().build();
        for (ComparisonExpression comp : fragCandidates) {

            // Get the table name and the attribute (form: table.attribute or attribute)
            DereferenceExpression left = (DereferenceExpression) comp.getLeft();
            List<String> parts = DereferenceExpression.getQualifiedName(left).getParts();
            String table, attr;
            if (parts.size() == 2) {
                table = parts.get(0);
                attr = parts.get(1);
            } else if (parts.size() == 1) {
                table = "";     // TODO get table accordingly
                attr = parts.get(0);
            } else
                throw new IllegalArgumentException("Cannot process DereferenceExpression: " + left + "\n Reason: " +
                        "QualifiedName of the expression is not of the form <table>.<attribute> or <attribute>");


            // Get the operator and the comparison value
            Operator op = comp.getOperator();
            Expression right = comp.getRight();
            long compvalue = 0;
            if (right instanceof LongLiteral) {
                LongLiteral longLit = (LongLiteral) right;
                compvalue = longLit.getValue();
            }


            // Query the fragmentation meta data table with the table and attribute name
            String sql = "SELECT ID,MINVALUE,MAXVALUE FROM FRAGMETA WHERE TABLE=? AND ATTRIBUTE=?";
            PreparedStatement prep;
            try {
                prep = conn.prepareStatement(sql);
                prep.setString(1, table);
                prep.setString(2, attr);
                ResultSet res = prep.executeQuery();
                while (res.next()) {

                    // if the frag
                    // TODO duplicate comparisons, e.g. T.A > 5 and T.A <= 10

                    // If the comparison and a fragment match, store the fragment id
                    if (matchCompAndFragMeta(op, compvalue, res.getLong(2), res.getLong(3))) {
                        frags.put(table + "." + attr, res.getInt(1));
                    }
                }
            } catch (SQLException e) {
                System.err.println("ERROR for PreparedStatement to query FRAGMETA in " + this.getClass().getName());
                e.printStackTrace();
            }


        }

        // return the found assignment for the comparisons
        return frags;

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



    private void testJoinsForCopartitions() {
        // TODO
    }


}
