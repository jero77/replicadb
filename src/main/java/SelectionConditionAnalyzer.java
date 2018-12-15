import com.facebook.presto.sql.tree.*;

import java.util.ArrayList;

public class SelectionConditionAnalyzer extends DefaultExpressionTraversalVisitor<Void, Void> {

    private ArrayList<ComparisonExpression> comparisons;

    private Expression lastAnalyzed;

//########################### Constructors ###################################

    public SelectionConditionAnalyzer() {
        super();
        comparisons = new ArrayList<ComparisonExpression>();
        lastAnalyzed = null;
    }


//########################### Overwritten Methods ###################################

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, Void context) {

        // Store in hashmap
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

    //########################### Analyzing Methods ###################################

    /**
     * Analyze a given {@link Expression} and print the results
     * @param e
     */
    public void analyzePrint(Expression e) {

        comparisons.clear();
        lastAnalyzed = e;

        process(e);

        System.out.println("Found the following comparisons in the expression: " + e);
        for (ComparisonExpression comp : comparisons) {
            System.out.println("Left: " + comp.getLeft() + ", Right: " + comp.getRight() + ", Operator: "
                    + comp.getOperator());
        }
    }


}
