using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EdFi.DataMartViews
{
    //TODO: How to identify a junk table
    //TODO: Create indexed views
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
            return $"{(schema != null ? "[" + schema + "]." : "")}[{tableName}]" + (columnName != null ? $".[{columnName}]" : "");
        }

        private string CastAndQualifyKeyColumn(string schema, string tableName, SchemaAnalyzer.ColumnDefinition column)
        {
            string columnName = ToQualified(schema, tableName, column.ColumnName);

            if (column.DataType != "nvarchar")
                columnName = $"CAST({ columnName} AS NVARCHAR(30))";

            return columnName;
        }

        private string GetViewKeyDefinition(string schema, string tableName, IEnumerable<SchemaAnalyzer.ColumnDefinition> columns)
        {
            return string.Join("\n\t\t+ '_' + ", columns.Select(_ => CastAndQualifyKeyColumn(schema, tableName, _)));
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

        private string ApplyLookupQualifier(string name)
        {
            bool isType = name.EndsWith("Type");
            bool isDescriptor = name.EndsWith("Descriptor");

            if (isType || isDescriptor)
            {
                name = name.Replace(isType ? "Type" : "Descriptor", "") + "Lookup";
            }

            return name;
        }

        private string GetCreateViewStatement(string viewName, string sql)
        {
            return $"CREATE VIEW [{_viewSchema}].[{viewName}] WITH SCHEMABINDING AS\n{sql} ;";
        }

        //TODO: Handle bridge table requirement
        /// <summary>
        /// A student can take the same assessment more than once. This is identified by a PK column that is not a FK.
        /// In this case a bridge table has to be created. THe bridge table is a factless-fact table. It serves only
        /// to count each student assessment.
        /// </summary>
        private string GetFactSql(string tableName)
        {
            var primaryKeyClause = GetViewKeyDefinition(_schema, tableName, _analyzer.GetPrimaryKeyColumns(tableName)) + " AS [" + tableName + "Key]";

            var relationships = _analyzer.GetForeignKeyRelationships(tableName);

            var foreignKeyClauses = relationships.Select(_ => GetViewKeyDefinition(null, _.PrimaryKeyCorrelationName, _.RelationshipColumns.Select(c => c.PrimaryKeyColumn)) + " AS [" + ApplyLookupQualifier(GetConventionBasedRelationshipName(_)) + "Key]");

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

        private string GetLookupSql(string tableName)
        {
            bool isDescriptor = tableName.EndsWith("Descriptor");

            string primaryKeyName = isDescriptor ? tableName.Replace("Descriptor", "Key") : tableName.Replace("Type", "Key");

            string primaryKeyClause = GetViewKeyDefinition(_schema, tableName, _analyzer.GetPrimaryKeyColumns(tableName)) + " AS [" + primaryKeyName + "]";

            string typeTableName = tableName.Replace("Descriptor", "Type");

            var sb = new StringBuilder();

            sb.Append($"SELECT\n\t{primaryKeyClause}, ");

            if (isDescriptor)
            {
                sb.Append(string.Join(",\n\t", _analyzer.GetNonKeyColumns(typeTableName).Select(_ => $"\n\t{ToQualified(_schema, typeTableName, _)}")));
            }
            else
            {
                sb.Append(string.Join(",\n\t", _analyzer.GetNonKeyColumns(tableName).Select(_ => ToQualified(_schema, tableName, _))));
            }

            sb.Append($"\nFROM [{_schema}].[{tableName}]");

            if (isDescriptor)
            {
                sb.Append($"\n\tJOIN [{_schema}].[{typeTableName}] ON\n\t\t[{_schema}].[{tableName}].[{typeTableName}Id] = [{_schema}].[{typeTableName}].[{typeTableName}Id]");
            }

            return sb.ToString();   
        }

        private string GetDimensionSql(string tableName)
        {
            var primaryKeyClause = GetViewKeyDefinition(_schema, tableName, _analyzer.GetPrimaryKeyColumns(tableName)) + " AS [" + tableName + "Key]";

            var relationships = _analyzer.GetForeignKeyRelationships(tableName);

            var foreignKeyClauses = relationships.SelectMany(r => r.RelationshipColumns.Select(c => ToQualified(null, r.PrimaryKeyCorrelationName, c.PrimaryKeyColumn.ColumnName) + " AS [" /*+ r.PrimaryKeyTable*/ + c.Column.ColumnName + "]"));

            var viewColumns = new List<string>();
            viewColumns.Add(primaryKeyClause);
            viewColumns.AddRange(foreignKeyClauses);
            viewColumns.AddRange(_analyzer.GetNonKeyColumns(tableName).Select(_ => ToQualified(_schema, tableName, _)));

            return "SELECT"
                + string.Join(", ", viewColumns.Select(_ => $"\n\t{_}"))
                + $"\nFROM [{_schema}].[{tableName}]"
                + string.Join("", relationships.Select(_ => $"\n{GetRelationshipJoin(tableName, _)}"));
        }

        public void ProcessFactTable(string tableName)
        {
            if (!_analyzer.TableExists(tableName))
                throw new ArgumentException($"Table {tableName} does not exist.");

            _dependentTables = new HashSet<string>();

            using (var context = GetContext())
            {
                SqlUtil.ExecuteCommand(_connectionString, GetCreateViewStatement("Fact" + tableName, GetFactSql(tableName)));
            }

            foreach (string dependentTable in _dependentTables)
                ProcessDimension(dependentTable);
        }


        //TODO: Multiple dimension tables may be created from a dependent table. See The Assessment dimension for an example
        private void ProcessDimension(string dependentTable)
        {
            using (var context = GetContext())
            {
                bool isDescriptor = dependentTable.EndsWith("Descriptor");
                bool isType = dependentTable.EndsWith("Type");

                if (isDescriptor || isType)
                {
                    string viewName = $"Lkp{dependentTable.Replace(isDescriptor ? "Descriptor" : "Type", "")}";
                    if (!_analyzer.ViewExists(viewName))
                    {
                        SqlUtil.ExecuteCommand(_connectionString, GetCreateViewStatement(viewName, GetLookupSql(dependentTable)));
                    }
                }
                else
                {
                    SqlUtil.ExecuteCommand(_connectionString, GetCreateViewStatement("Dim" + dependentTable, GetDimensionSql(dependentTable)));
                }
            }
        }
    }
}
