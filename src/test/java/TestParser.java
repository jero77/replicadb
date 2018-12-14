import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;

import java.util.Optional;

public class TestParser {

    public static void main(String[] args) {


        SqlParser parser = new SqlParser();
        String sql = "SELECT * FROM TA, TB WHERE TA.AGE <= 35 AND TA.ID = TB.IDOFTA";

        Query query = (Query) parser.createStatement(sql, new ParsingOptions());
        QuerySpecification body = (QuerySpecification)query.getQueryBody();

        Select select = body.getSelect();
        Optional<Expression> where = body.getWhere();
        if(where.isPresent())
            System.out.print("Where clause is contained: ");
            Expression expr = where.get();
        System.out.println(expr);
    }

}
