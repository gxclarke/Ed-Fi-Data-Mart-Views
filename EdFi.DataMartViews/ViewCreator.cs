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
        string _lookupSchema;
        SchemaAnalyzer _analyzer;
        HashSet<string> _factDependentTables;
        HashSet<string> _dimensionDependentTables;

        public ViewCreator(string connectionString, string schema, string viewSchema, string lookupSchema = null)
        {
            _connectionString = connectionString;
            _schema = schema;
             _viewSchema = viewSchema;
            _lookupSchema = lookupSchema ?? viewSchema;

            _analyzer = new SchemaAnalyzer(connectionString, schema);
        }

        private DbContext GetContext()
        {
            return SqlUtil.GetContext(_connectionString);
        }

        private string ToQualified(string schema, string tableName, string columnName = null)
        {
            return $"{(schema != null ? "[" + schema + "]." : "")}[{tableName}]" + (columnName != null ? $".[{columnName}]" : "");
        }

        private string CastAndQualifySurrogateKeyColumn(string schema, string tableName, SchemaAnalyzer.ColumnDefinition column)
        {
            string columnName = ToQualified(schema, tableName, column.ColumnName);

            if (column.DataType == "date")
                columnName = $"CONVERT(NVARCHAR(8), {columnName}, 112)";
            else if (column.DataType != "nvarchar")
                columnName = $"CAST({columnName} AS NVARCHAR(30))";

            return columnName;
        }

        private string GetSurrogateKeyDefinition(string schema, string tableName, IEnumerable<SchemaAnalyzer.ColumnDefinition> columns)
        {
            return string.Join("\n\t\t+ '_' + ", columns.Select(_ => CastAndQualifySurrogateKeyColumn(schema, tableName, _)));
        }

        private string GetRelationshipJoin(string tableName, SchemaAnalyzer.ForeignKeyRelationship relationship)
        {
            var joinClauses = relationship.RelationshipColumns.Select(_ => 
                $"{ToQualified(_schema, tableName, _.Column.ColumnName)} = {ToQualified(null, relationship.PrimaryKeyCorrelationName, _.PrimaryKeyColumn.ColumnName)}");

            return $"\tLEFT JOIN {ToQualified(_schema, relationship.PrimaryKeyTable)} AS [{relationship.PrimaryKeyCorrelationName}] ON\n\t\t{string.Join(" AND\n\t\t", joinClauses)}";
        }

        private string GetConventionBasedRelationshipName(SchemaAnalyzer.ForeignKeyRelationship relationship)
        {
            if (relationship.RelationshipColumns.Count == 1)
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

        private string ApplyLookupColumnQualifier(string columnName)
        {
            bool isColumn = columnName.EndsWith("Id");

            string typeSuffix = "Type" + (isColumn ? "Id" : "");
            string descriptorSuffix = "Descriptor" + (isColumn ? "Id" : "");

            bool isType = columnName.EndsWith(typeSuffix);
            bool isDescriptor = columnName.EndsWith(descriptorSuffix);

            if (isType || isDescriptor)
            {
                columnName = columnName.Replace(isType ? typeSuffix : descriptorSuffix, "") + "Lookup";
            }

            return columnName;
        }

        private void CreateView(string schema, string viewName, string sql)
        {
            string statement =  $"CREATE VIEW {ToQualified(schema, viewName)} WITH SCHEMABINDING AS\n{sql};";
            SqlUtil.ExecuteCommand(_connectionString, statement);
        }

        private void CreateClusteredIndex(string viewName, string primaryKeyColumnName)
        {
            return; // This won't work because most of our views contain left joins.
            string statement = $"CREATE UNIQUE CLUSTERED INDEX CX_{viewName} ON {ToQualified(_viewSchema, viewName)} ([{primaryKeyColumnName}]);";
            SqlUtil.ExecuteCommand(_connectionString, statement);
        }

        private void CreateFactTableView(string tableName)
        {
            var primaryKeyColumns = _analyzer.GetPrimaryKeyColumns(tableName);

            string primaryKeyColumnName = tableName + "Key";

            string primaryKeyColumnClause = GetSurrogateKeyDefinition(_schema, tableName, primaryKeyColumns) + " AS [" + primaryKeyColumnName + "]";

            var relationships = _analyzer.GetForeignKeyRelationships(tableName);

            var foreignKeyColumnClauses = relationships.Select(_ => GetSurrogateKeyDefinition(null, _.PrimaryKeyCorrelationName, 
                _.RelationshipColumns.Select(c => c.PrimaryKeyColumn)) + " AS [" + ApplyLookupColumnQualifier(GetConventionBasedRelationshipName(_)) + "Key]");

            foreach (var relationship in relationships)
            {
                if (!_factDependentTables.Contains(relationship.PrimaryKeyTable))
                    _factDependentTables.Add(relationship.PrimaryKeyTable);
            }

            var viewColumns = new List<string>();
            viewColumns.Add(primaryKeyColumnClause);
            viewColumns.AddRange(foreignKeyColumnClauses);
            viewColumns.AddRange(_analyzer.GetNonKeyColumns(tableName).Select(_ => ToQualified(_schema, tableName, _)));

            string sql = "SELECT"
                + string.Join(", ", viewColumns.Select(_ => $"\n\t{_}"))
                + $"\nFROM [{_schema}].[{tableName}]"
                + string.Join("", relationships.Select(_ => $"\n{GetRelationshipJoin(tableName, _)}"));

            string viewName = "Fact" + tableName;

            CreateView(_viewSchema, viewName, sql);
            CreateClusteredIndex(viewName, primaryKeyColumnName);
        }

        private void CreateDimensionView(string tableName)
        {
            bool asLookup = tableName.EndsWith("Descriptor") || tableName.EndsWith("Type");
            string schema = asLookup ? _lookupSchema : _viewSchema;
            string viewName = (asLookup ? "Lkp" : "Dim") + tableName;

            if (_analyzer.ViewExists(schema, viewName))
                return;

            var primaryKeyColumns = _analyzer.GetPrimaryKeyColumns(tableName);

            string primaryKeyColumnName = asLookup ? tableName.Replace("Descriptor", "Key").Replace("Type", "Key") : tableName + "Key";

            string primaryKeyColumnClause = GetSurrogateKeyDefinition(_schema, tableName, primaryKeyColumns) + " AS [" + primaryKeyColumnName + "]";

            var relationships = _analyzer.GetForeignKeyRelationships(tableName);

            var joinClauses = new List<string>();

            var foreignKeyColumnClauses = new List<string>();
            foreach (var relationship in relationships)
            {
                bool processed = false;
                if (relationship.RelationshipColumns.Count == 1)
                {
                    if (!asLookup)
                    {
                        var relationshipColumn = relationship.RelationshipColumns[0];
                        string lookupQualifiedColumnName = ApplyLookupColumnQualifier(relationshipColumn.Column.ColumnName);
                        if (relationshipColumn.Column.ColumnName != lookupQualifiedColumnName)
                        {
                            foreignKeyColumnClauses.Add(ToQualified(null, relationship.PrimaryKeyCorrelationName, relationshipColumn.PrimaryKeyColumn.ColumnName) + " AS [" + lookupQualifiedColumnName + "Key]");
                            _dimensionDependentTables.Add(relationship.PrimaryKeyTable);
                            processed = true;
                        }
                    }
                }
                if (!processed)
                {
                    var relatedTableColumns = _analyzer.GetNonKeyColumns(relationship.PrimaryKeyTable);
                    foreignKeyColumnClauses.AddRange(relatedTableColumns.Select(c 
                        => ToQualified(null, relationship.PrimaryKeyCorrelationName, c) + " AS [" + relationship.PrimaryKeyCorrelationName + c + "]"));
                }
                joinClauses.Add(GetRelationshipJoin(tableName, relationship));
            }

            var viewColumns = new List<string>();
            viewColumns.Add(primaryKeyColumnClause);
            viewColumns.AddRange(foreignKeyColumnClauses);
            viewColumns.AddRange(_analyzer.GetNonKeyColumns(tableName).Select(_ => ToQualified(_schema, tableName, _)));

            string sql = "SELECT"
                + string.Join(", ", viewColumns.Select(_ => $"\n\t{_}"))
                + $"\nFROM [{_schema}].[{tableName}]\n"
                + string.Join("\n", joinClauses);

            CreateView(schema, viewName, sql);
            CreateClusteredIndex(viewName, primaryKeyColumnName);
        }

        public void ProcessFactTable(string tableName)
        {
            if (!_analyzer.TableExists(tableName))
                throw new ArgumentException($"Table {tableName} does not exist.");

            _factDependentTables = new HashSet<string>();

            using (var context = GetContext())
                CreateFactTableView(tableName);

            _dimensionDependentTables = new HashSet<string>();

            foreach (string factDependentTable in _factDependentTables)
                CreateDimensionView(factDependentTable);

            foreach (string dimensionDependentTable in _dimensionDependentTables)
                CreateDimensionView(dimensionDependentTable);
        }
    }
}
