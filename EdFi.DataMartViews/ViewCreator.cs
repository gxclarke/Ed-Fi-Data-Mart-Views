using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EdFi.DataMartViews
{
    public class ViewCreator
    {
        string _connectionString;
        string _schema;
        string _viewSchema;
        SchemaAnalyzer _analyzer;
        HashSet<string> _dependentTables;

        public ViewCreator(string connectionString, string schema, string viewSchema)
        {
            _connectionString = connectionString;
            _schema = schema;
            _viewSchema = viewSchema;
            _analyzer = new SchemaAnalyzer(connectionString, schema);
        }

        private DbContext GetContext()
        {
            return SqlUtil.GetContext(_connectionString);
        }

        private string ToQualified(string schema, string tableName, string columnName = null)
        {
            return $"[{schema}].[{tableName}]" + (columnName != null ? $".[{columnName}]" : "");
        }

        private string CastAndQualifyKeyColumn(string tableName, SchemaAnalyzer.ColumnDefinition column)
        {
            string columnName = ToQualified(_schema, tableName, column.ColumnName);

            if (column.DataType != "nvarchar")
                columnName = $"CAST({ columnName} AS NVARCHAR(30))";

            return columnName;
        }

        private string GetViewKeyDefinition(string tableName, List<SchemaAnalyzer.ColumnDefinition> columns)
        {
            return string.Join("\n\t+ '_' + ", columns.Select(_ => CastAndQualifyKeyColumn(tableName, _)));
        }

        private string GetRelationshipJoin(string tableName, SchemaAnalyzer.ForeignKeyRelationship relationship)
        {
            var joinClauses = relationship.Columns.Zip(relationship.PrimaryKeyColumns, (c, pkc) => 
                $"{ToQualified(_schema, tableName, c.ColumnName)} = {ToQualified(_schema, relationship.PrimaryKeyTable, pkc.ColumnName)}");

            return $"\tLEFT JOIN {ToQualified(_schema, relationship.PrimaryKeyTable)} ON {string.Join(" AND\n\t\t", joinClauses)}";
        }

        private string ResolveRelationshipName(SchemaAnalyzer.ForeignKeyRelationship relationship)
        {
            if (relationship.Columns.Count == 1)
            {
                int i = relationship.ConstraintName.LastIndexOf('_');
                if (i != -1)
                {
                    string afterLastUnderscore = relationship.ConstraintName.Substring(i + 1);
                    if (afterLastUnderscore.EndsWith("Id"))
                        return afterLastUnderscore.Substring(0, afterLastUnderscore.Length - 2);
                }
            }
            return relationship.PrimaryKeyTable;
        }

        private string ApplyLookupQualifier(string name)
        {
            if (name.EndsWith("Type") || name.EndsWith("Descriptor"))
                name += "Lookup";

            return name;
        }

        //TODO: Handle bridge table requirement
        /// <summary>
        /// A student can take the same assessment more than once. This is identified by a PK column that is not a FK.
        /// In this case a bridge table has to be created. THe bridge table is a factless-fact table. It serves only
        /// to count each student assessment.
        /// </summary>
        private string GetFactSql(string tableName)
        {
            var primaryKeyClause = GetViewKeyDefinition(tableName, _analyzer.GetPrimaryKeyColumns(tableName)) + " AS [" + tableName + "Key]";

            var relationships = _analyzer.GetForeignKeyRelationships(tableName);

            var foreignKeyClauses = relationships.Select(_ => GetViewKeyDefinition(tableName, _.Columns) + " AS [" + ApplyLookupQualifier(ResolveRelationshipName(_)) + "Key]");

            foreach (var relationship in relationships)
            {
                if (!_dependentTables.Contains(relationship.PrimaryKeyTable))
                    _dependentTables.Add(relationship.PrimaryKeyTable);
            }

            var viewColumns = new List<string>();
            viewColumns.Add(primaryKeyClause);
            viewColumns.AddRange(foreignKeyClauses);
            viewColumns.AddRange(_analyzer.GetNonKeyColumns(tableName).Select(_ => ToQualified(_schema, tableName, _)));

            return "SELECT"
                + string.Join(", ", viewColumns.Select(_ => $"\n\t{_}"))
                + $"\nFROM [{_schema}].[{tableName}]"
                + string.Join("", relationships.Select(_ => $"\n{GetRelationshipJoin(tableName, _)}"));
        }

        private string GetDescriptorSql(string tableName)
        {
            var primaryKeyClause = GetViewKeyDefinition(tableName, _analyzer.GetPrimaryKeyColumns(tableName)) + " AS [" + tableName.Replace("Descriptor", "") + "Key]";

            string typeTableName = tableName.Replace("Descriptor", "Type");
            var n = _analyzer.GetNonKeyColumns(typeTableName);

            return "SELECT"
                + $"\n\t{primaryKeyClause}, "
                + string.Join(", ", _analyzer.GetNonKeyColumns(typeTableName).Select(_ => $"\n\t{ToQualified(_schema, typeTableName, _)}"))
                + $"\nFROM [{_schema}].[{tableName}]"
                + $"\n\tJOIN [{_schema}].[{typeTableName}] ON\n\t\t[{_schema}].[{tableName}].[{typeTableName}Id] = [{_schema}].[{typeTableName}].[{typeTableName}Id]";     
        }

        public void ProcessFactTable(string factSourceTable)
        {
            if (!_analyzer.TableExists(factSourceTable))
                throw new ArgumentException($"Table {factSourceTable} does not exist.");

            _dependentTables = new HashSet<string>();

            using (var context = GetContext())
            {
                string sql = $"CREATE VIEW [{_viewSchema}].[Fact{factSourceTable}] AS\n{GetFactSql(factSourceTable)};";
                SqlUtil.ExecuteCommand(_connectionString, sql);
            }

            foreach (string dependentTable in _dependentTables)
                ProcessDimension(dependentTable);
        }

        //TODO: Multiple dimension tables may be created from a dependent table. See The Assessment dimension for an example
        private void ProcessDimension(string dependentTable)
        {
            using (var context = GetContext())
            {
                if (dependentTable.EndsWith("Descriptor"))
                {
                    string viewName = $"Lkp{dependentTable}";
                    if (!_analyzer.ViewExists(viewName))
                    {
                        string sql = $"CREATE VIEW [{_viewSchema}].[{viewName}] AS\n{GetDescriptorSql(dependentTable)};";
                        SqlUtil.ExecuteCommand(_connectionString, sql);
                    }
                }
            }
        }
    }
}
