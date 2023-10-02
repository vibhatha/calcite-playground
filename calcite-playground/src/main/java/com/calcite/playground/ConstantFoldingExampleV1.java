package com.calcite.playground;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.util.List;

public class ConstantFoldingExampleV1 {

    public static void main(String[] args) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        RuleSet ruleSet = RuleSets.ofList(CoreRules.CALC_REDUCE_EXPRESSIONS);

        // Configure the planner
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .traitDefs((List<RelTraitDef>) null)
                .ruleSets(ruleSet)
                .build();

        Planner planner = Frameworks.getPlanner(config);

        try {
            String sql = "SELECT 3+4, UPPER('abc') FROM (VALUES(0))";
            // Parse the query
            SqlNode parse = planner.parse(sql);
            // Validate the query
            SqlNode validate = planner.validate(parse);
            // Convert the query to relational form
            RelNode rel = planner.rel(validate).project();

            // Display the relational form
            System.out.println(RelOptUtil.toString(rel));

            // To get the optimized SQL back
            SqlDialect dialect = SqlDialect.DatabaseProduct.MYSQL.getDialect();
            RelToSqlConverter converter = new RelToSqlConverter(dialect);
            SqlImplementor.Result optimizedSql = converter.visit(rel);
            System.out.println(optimizedSql.toString());

        } catch (SqlParseException | ValidationException | RelConversionException e) {
            e.printStackTrace();
        }
    }
}
