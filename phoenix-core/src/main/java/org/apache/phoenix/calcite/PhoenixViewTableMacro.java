/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
public class PhoenixViewTable extends ViewTable {

  static */class PhoenixViewTableMacro implements TableMacro {
  private final String viewSql;
  private final SchemaPlus schemaPlus;
  private final Boolean modifiable;
  /** Typically null. If specified, overrides the path of the schema as the
   * context for validating {@code viewSql}. */
  private final List<String> schemaPath;
  private final List<String> viewPath;

    PhoenixViewTableMacro(SchemaPlus schemaPlus, String viewSql, List<String> schemaPath,
        List<String> viewPath, Boolean modifiable) {
      this.viewSql = viewSql;
      this.schemaPlus = schemaPlus;
      this.viewPath = viewPath == null ? null : ImmutableList.copyOf(viewPath);
      this.modifiable = modifiable;
      this.schemaPath =
          schemaPath == null ? null : ImmutableList.copyOf(schemaPath);
    }

    public List<FunctionParameter> getParameters() {
      return Collections.emptyList();
    }

    /**
     *
     * @param arguments
     * 1. schema name
     * 2. view name
     * 3. PhoenixConnection
     * @return a ViewTable or ModifiableViewTable.
     */
    public TranslatableTable apply(List<Object> arguments) {
      final CalciteSchema schema = CalciteSchema.from(schemaPlus);
      if (modifiable) {
        try {
          final String schemaName = (String) arguments.get(0);
          final String viewName = (String) arguments.get(1);
          final PhoenixConnection pc = (PhoenixConnection) arguments.get(2);
          final TableName tableName = TableName.create(schemaName, viewName);
          final ColumnResolver resolver = FromCompiler.getResolver(
              NamedTableNode.create(
                  null,
                  tableName,
                  ImmutableList.<ColumnDef>of()), pc);
          // TODO: don't need resolver. just ensure that view sql at earlier stage uses dynamic columns
          // then we can use the parsed AnalyzeViewResult all the way through
          // okay if theop
          final Table viewTable = new PhoenixTable(pc, resolver.getTables().get(0));
          final CalcitePrepare.AnalyzeViewResult parsed =
              Schemas.analyzeView(MaterializedViewTable.MATERIALIZATION_CONNECTION,
                  schema, schemaPath, viewSql, modifiable);
          final List<String> schemaPath1 =
              schemaPath != null ? schemaPath : schema.path(null);
          final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
          final Type elementType = typeFactory.getJavaClass(parsed.rowType);
          ImmutableList<Integer> viewColumnMapping =
              new ImmutableList.Builder<Integer>()
                  .addAll(parsed.columnMapping)
                  .build();
          if (parsed.table instanceof Wrapper) {
            ImmutableList.Builder<Integer> newViewColumnMapping = ImmutableList.builder();
            final PhoenixTable pTable = ((Wrapper) parsed.table).unwrap(PhoenixTable.class);
            final PColumn tenantIdCol =
                pTable.tableMapping.getTableRef().getTable().getPKColumns().get(0);
            for (Integer tableIndex : parsed.columnMapping.subList(0, parsed.columnMapping.size())) {
              if (tableIndex != tenantIdCol.getPosition()) {// FIXME: only remove if tenant-specific table
                newViewColumnMapping.add(tableIndex);
              }
            }
            viewColumnMapping = newViewColumnMapping.build();
          }
          return new ModifiableViewTable(elementType,
              RelDataTypeImpl.proto(viewTable.getRowType(typeFactory)), viewSql, schemaPath1, viewPath,
              viewTable,
              Schemas.path(schema.root(),  parsed.tablePath),
              parsed.constraint,
              ImmutableIntList.copyOf(viewColumnMapping), typeFactory);
        } catch (SQLException e) {
          // Use the default ViewTableMacro which resolves based on the metadata of the underlying
          // table instead of the stored-metadata of the view.
        }
      }
      final TableMacro viewMacro =
          ViewTable.viewMacro(schema, viewSql, schemaPath, viewPath, false);
      return viewMacro.apply(ImmutableList.of());
    }
  }
/*
}
*/