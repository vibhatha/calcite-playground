package com.calcite.playground;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.tools.Frameworks;

public class InMemoryTable extends AbstractTable {

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        // Define the schema of the table
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("ID", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER));
        builder.add("NAME", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
        return builder.build();
    }

    // ... Other methods to provide data, if necessary ...
}