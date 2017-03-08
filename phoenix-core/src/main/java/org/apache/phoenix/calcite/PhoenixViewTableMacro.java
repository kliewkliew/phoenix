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
import org.apache.calcite.util.Util;
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
  private final List<String> schemaPath;
  private final List<String> viewPath;
  private final String schemaName;
  private final PhoenixConnection pc;

  PhoenixViewTableMacro(SchemaPlus schemaPlus, String viewSql, List<String> schemaPath,
          List<String> viewPath, Boolean modifiable, String schemaName, PhoenixConnection pc) {
    this.viewSql = viewSql;
    this.schemaPlus = schemaPlus;
    this.viewPath = viewPath == null ? null : ImmutableList.copyOf(viewPath);
    this.modifiable = modifiable;
    this.schemaPath = schemaPath == null ? null : ImmutableList.copyOf(schemaPath);
    this.schemaName = schemaName;
    this.pc = pc;
  }

  public List<FunctionParameter> getParameters() {
    return Collections.emptyList();
  }

  public TranslatableTable apply(List<Object> arguments) {
    final CalciteSchema schema = CalciteSchema.from(schemaPlus);
    if (modifiable) {
      try {
        final String viewName = Util.last(viewPath);
        final TableName tableName = TableName.create(schemaName, viewName);
        final ColumnResolver resolver = FromCompiler.getResolver(
                NamedTableNode.create(
                        null,
                        tableName,
                        ImmutableList.<ColumnDef>of()), pc);
        // If we use viewTable, the table is resolved to PhoenixTable on the view.
        // If we use parsed.table, the table is resolved to ViewTable with viewSql describing the
        //  relation to the underlying table.
        // We need to use the view table rather than the underlying table because
        //  in PhoenixTableModifyRule.convert() we unwrap PhoenixTable so that we can implement
        //  the rule based on PhoenixTable (ViewTable does not provide enough information).
        final CalcitePrepare.AnalyzeViewResult parsed =
                Schemas.analyzeView(MaterializedViewTable.MATERIALIZATION_CONNECTION,
                        schema, schemaPath, viewSql, modifiable);
        final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
        final Table viewTable = new PhoenixTable(pc, resolver.getTables().get(0), typeFactory);
        final List<String> schemaPath1 =
                schemaPath != null ? schemaPath : schema.path(null);
        final Type elementType = typeFactory.getJavaClass(parsed.rowType);
        return new ModifiableViewTable(
                elementType, RelDataTypeImpl.proto(viewTable.getRowType(typeFactory)), viewSql,
                schemaPath1, viewPath, viewTable, Schemas.path(schema.root(),  parsed.tablePath),
                parsed.constraint, parsed.columnMapping, typeFactory);
      } catch (SQLException e) {
        // Use the default ViewTableMacro which resolves based on the metadata of the underlying
        // table instead of the stored-metadata of the view.
      }
    }
    final TableMacro viewMacro =
            ViewTable.viewMacro(schemaPlus, viewSql, schemaPath, viewPath, false);
    return viewMacro.apply(ImmutableList.of());
  }
}
/*
}
*/