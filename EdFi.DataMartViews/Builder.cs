using Microsoft.EntityFrameworkCore;
using SqlServer.Core.InformationSchema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EdFi.DataMartViews
{
    public class Builder
    {
        string _connectionString;
        string _dataMartSchemaOwner;
        string _schema;
        string _lookupSchema;

        SchemaAnalyzer _analyzer;

        Dictionary<string, ViewDefinition> _tableViewDefinitions;

        static readonly string[] _lookupIdentifiers = { "Descriptor", "Type" };

        public Builder(string connectionString, string dataMartSchemaOwner, string schema, string lookupSchema = null)
        {
            _connectionString = connectionString;
            _schema = schema;
            _lookupSchema = lookupSchema;
            _dataMartSchemaOwner = dataMartSchemaOwner;

            if (lookupSchema != null)
            {
                CreateSchema(lookupSchema);
                DropExistingViews(lookupSchema);
            }

            _analyzer = new SchemaAnalyzer(_connectionString, _schema);
        }

        public void RecreateDataMart(string viewSchema, string[] factSourceTables)
        {
            CreateSchema(viewSchema);
            DropExistingViews(viewSchema);

            _tableViewDefinitions = new Dictionary<string, ViewDefinition>();

            foreach (string factSourceTable in factSourceTables)
            {
                AddViewDefinitionRecursive(factSourceTable, ViewType.Fact);
            }

            var viewCreator = new ViewCreator(_connectionString, _schema, viewSchema, _lookupSchema);

            foreach(var viewDefinition in _tableViewDefinitions)
            {
                viewCreator.CreateView(viewDefinition.Key, viewDefinition.Value);
            }
        }

        private void CreateSchema(string schema)
        {
            using (var context = new SchemaDbContext(_connectionString))
            {
                if (context.Schemata.SingleOrDefault(_ => _.SchemaName == schema) == null)
                {
                    string sql = $"CREATE SCHEMA [{schema}] AUTHORIZATION [{_dataMartSchemaOwner}];";
                    SqlUtil.ExecuteCommand(_connectionString, sql);
                }
            }
        }

        private void DropExistingViews(string schema)
        {
            using (var context = new SchemaDbContext(_connectionString))
            {
                foreach (var view in context.Views.Where(_ => _.TableSchema == schema).ToList())
                {
                    string sql = "DROP VIEW [" + view.TableSchema + "].[" + view.TableName + "];";
                    SqlUtil.ExecuteCommand(_connectionString, sql);
                }
            }
        }

        private void AddViewDefinitionRecursive(string tableName, ViewType? type = null)
        {
            var viewDefinition = GetViewDefinition(tableName, type);

            _tableViewDefinitions.Add(tableName, viewDefinition);

            foreach (string primaryKeyTable in viewDefinition.GetPrimaryKeyTables())
            {
                if (!_tableViewDefinitions.ContainsKey(primaryKeyTable))
                {
                    AddViewDefinitionRecursive(primaryKeyTable);
                }
            }
        }

        private ViewDefinition GetViewDefinition(string tableName, ViewType? type = null)
        {
            if (type == null)
            {
                type = _lookupIdentifiers.Any(tableName.EndsWith) ? ViewType.Lookup : ViewType.Dimension;
            }

            var viewDefinition = new ViewDefinition(tableName, type.Value);

            viewDefinition.ColumnGroups.Add(new ViewDefinition.PrimaryKey { Columns = _analyzer.GetPrimaryKeyColumns(tableName) });

            foreach (var relationship in _analyzer.GetForeignKeyRelationships(tableName))
            {
                var keyType = ViewDefinition.ReferenceKeyType.DimensionReference;
                if (type == ViewType.Dimension || type == ViewType.Lookup)
                    keyType = ViewDefinition.ReferenceKeyType.Denormalized;
                else if (_lookupIdentifiers.Any(relationship.PrimaryKeyTable.EndsWith))
                    keyType = ViewDefinition.ReferenceKeyType.LookupReference;

                viewDefinition.ColumnGroups.Add(new ViewDefinition.ReferenceKey
                {
                    Relationship = relationship,
                    KeyType = keyType,
                    Columns = keyType == ViewDefinition.ReferenceKeyType.Denormalized 
                        ? _analyzer.GetNonKeyColumns(relationship.PrimaryKeyTable) 
                        : new List<SchemaAnalyzer.ColumnDefinition>()
                });
            };

            viewDefinition.ColumnGroups.Add(new ViewDefinition.ColumnGroupDefinition { Columns = _analyzer.GetNonKeyColumns(tableName) });

            return viewDefinition;
        }

    }
}
