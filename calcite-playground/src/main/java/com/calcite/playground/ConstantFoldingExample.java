package com.calcite.playground;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import org.apache.calcite.rel.rules.CoreRules;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;

import org.apache.calcite.rel.rules.ReduceExpressionsRule;


import java.util.List;

public class ConstantFoldingExample {

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        // Create an instance of your table
        InMemoryTable myTable = new InMemoryTable();

        // Create a sub-schema called 'DUMMY' and add your table to it
        SchemaPlus dummySchema = rootSchema.add("DUMMY", new AbstractSchema());
        dummySchema.add("MYTABLE", myTable);

        RuleSet ruleSet = RuleSets.ofList(
                CoreRules.FILTER_REDUCE_EXPRESSIONS,
                CoreRules.CALC_REDUCE_EXPRESSIONS,
                CoreRules.PROJECT_REDUCE_EXPRESSIONS,
                CoreRules.WINDOW_REDUCE_EXPRESSIONS,
                CoreRules.JOIN_REDUCE_EXPRESSIONS
        );

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .ruleSets(ruleSet)
                .build();

        Planner planner = Frameworks.getPlanner(config);

        try {
            String sql = "SELECT 3+4";
            SqlNode parse = planner.parse(sql);
            SqlNode validate = planner.validate(parse);
            RelNode rel = planner.rel(validate).project();


            // Perform optimization
            RelTraitSet desiredTraits = rel.getTraitSet();
            RelTraitSet toTraits = rel.getTraitSet().simplify();
            toTraits.replace(Convention.NONE);

            RelNode optimizedRel = planner.transform(0, toTraits, rel);

            // Convert optimized RelNode to SQL
            RelToSqlConverter converter = new RelToSqlConverter(null);
            SqlImplementor.Result result = converter.visit(rel);
            SqlNode optimizedSqlNode = result.asQueryOrValues();
            String optimizedSql = optimizedSqlNode.toString();//toSqlString(RelToSqlConverter.DEFAULT_DIALECT).getSql();
            System.out.println(optimizedSql);
        } catch (SqlParseException | ValidationException | RelConversionException e) {
            e.printStackTrace();
        }
    }
}

