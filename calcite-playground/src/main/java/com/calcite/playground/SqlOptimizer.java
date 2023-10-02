package com.calcite.playground;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

public class SqlOptimizer {

    public static void main(String[] args) {
        // Step 1: Instantiate a FrameworkConfig
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        // Create an instance of your table
        InMemoryTable myTable = new InMemoryTable();

        // Create a sub-schema called 'DUMMY' and add your table to it
        SchemaPlus dummySchema = rootSchema.add("DUMMY", new AbstractSchema());
        dummySchema.add("MYTABLE", myTable);

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .build();

        // Step 2: Get a reference to a Planner
        Planner planner = Frameworks.getPlanner(config);

        String sql = "SELECT 4 + 3";

        try {
            // Step 3: Parse the SQL query
            SqlNode parsedSql = planner.parse(sql);

            // Step 4: Validate the parsed SQL query
            SqlNode validatedSql = planner.validate(parsedSql);

            // Step 5: Convert the validated SQL to a RelNode
            RelRoot relRoot = planner.rel(validatedSql);
            RelNode rel = relRoot.project();

            // Step 6: Optimize the RelNode
            RelTraitSet traitSet = rel.getTraitSet().replace(Convention.NONE);
            RelOptPlanner optPlanner = rel.getCluster().getPlanner();
            RelNode optimizedRel = optPlanner.findBestExp();
            System.out.println(optimizedRel.toString());

            // The optimizedRel now represents the optimized form of the query.
            // To get the SQL representation of the optimizedRel, you might need to convert it back to a SqlNode.
            // ...

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


