using Microsoft.EntityFrameworkCore;
using SqlServer.Core.InformationSchema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EdFi.DataMartViews
{
    public class SchemaAnalyzer
    {
        string _connectionString;
        string _schema;

        public SchemaAnalyzer(string connectionString, string schema)
        {
            _connectionString = connectionString;
            _schema = schema;
        }

        public struct ColumnDefinition
        {
            public string ColumnName { get; set; }
            public string DataType { get; set; }
        }

        private SchemaDbContext GetContext()
        {
            return SqlUtil.GetContext(_connectionString);
        }

        private List<ColumnDefinition> GetColumns(string tableName, string constraintName)
        {
            using (var context = GetContext())
            {
                var query = context.KeyColumnUsages
                    .Where(_ => _.TableSchema == _schema && _.ConstraintName == constraintName)
                    .Join(context.Columns, kcu => new { kcu.TableSchema, kcu.TableName, kcu.ColumnName }, c => new { c.TableSchema, c.TableName, c.ColumnName }, (kcu, c) => new { kcu, c })
                    .ToList(); // I don't know why it fails without this
                
                // Really weird issue _kcu.ColumnName returns the same value for each row. Using c.ColumnName instead avoids.
                return query.OrderBy(_ => _.c.OrdinalPosition)
                    .Select(_ => new ColumnDefinition { ColumnName = _.c.ColumnName, DataType = _.c.DataType })
                    .ToList();
            }
        }

        public List<ColumnDefinition> GetPrimaryKeyColumns(string tableName)
        {
            using (var context = GetContext())
            {
                var primaryKeyContraint = context.TableConstraints.Single(_ => _.TableSchema == _schema && _.TableName == tableName && _.ConstraintType == "PRIMARY KEY");

                return GetColumns(tableName, primaryKeyContraint.ConstraintName);
            }
        }

        public List<string> GetNonKeyColumns(string tableName)
        {
            using (var context = GetContext())
            {
                return context.Columns
                    .Where(_ => _.TableSchema == _schema && _.TableName == tableName)
                    .OrderBy(_ => _.OrdinalPosition)
                    .GroupJoin(context.KeyColumnUsages, c => new { c.TableSchema, c.TableName, c.ColumnName }, kcu => new { kcu.TableSchema, kcu.TableName, kcu.ColumnName }, (c, kcu) => new { Column = c, Count = kcu.Count() })
                    .Where(group => group.Count == 0)
                    .Select(group => group.Column.ColumnName)
                    .ToList();
            }
        }

        public class ForeignKeyColumn
        {
            public ColumnDefinition Column { get; set; }
            public ColumnDefinition PrimaryKeyColumn { get; set; }
        }

        public class ForeignKeyRelationship
        {
            public string ConstraintName { get; set; }
            public string PrimaryKeyTable { get; set; }
            public string PrimaryKeyCorrelationName { get; set; }
            public List<ForeignKeyColumn> RelationshipColumns { get; set; }
        }

        public List<ForeignKeyRelationship> GetForeignKeyRelationships(string tableName)
        {
            var relationships = new List<ForeignKeyRelationship>();

            using (var context = GetContext())
            {
                foreach (var foreignKeyConstraint in context.TableConstraints.Where(_ => _.TableSchema == _schema && _.TableName == tableName && _.ConstraintType == "FOREIGN KEY").ToList())
                {
                    var referentialConstraint = context.ReferentialConstraints.Single(_ => _.ConstraintSchema == foreignKeyConstraint.ConstraintSchema && _.ConstraintName == foreignKeyConstraint.ConstraintName);
                    var primaryKeyConstraint = context.TableConstraints.Single(_ => _.TableSchema == referentialConstraint.UniqueConstraintSchema && _.ConstraintName == referentialConstraint.UniqueConstraintName);
                    
                    var columns = GetColumns(tableName, referentialConstraint.ConstraintName);
                    var primaryKeyColumns = GetColumns(primaryKeyConstraint.TableName, primaryKeyConstraint.ConstraintName);

                    FixRelationshipColumnOrder(referentialConstraint, ref columns, ref primaryKeyColumns);

                    relationships.Add(new ForeignKeyRelationship
                    {
                        ConstraintName = foreignKeyConstraint.ConstraintName,
                        PrimaryKeyTable = primaryKeyConstraint.TableName,
                        PrimaryKeyCorrelationName = GetConventionBasedCorrelationName(foreignKeyConstraint.ConstraintName),
                        RelationshipColumns = columns.Zip(primaryKeyColumns, (c, pkc) => new ForeignKeyColumn {  Column = c, PrimaryKeyColumn = pkc }).ToList()
                    });
                }
            }

            return relationships;
        }

        private string GetConventionBasedCorrelationName(string constraintName)
        {
            string[] parts = constraintName.Split('_');
            if (parts.Length == 3)
                return parts[2].Replace("Id", "");
            else
                return parts[2];
        }

        private void FixRelationshipColumnOrder(ReferentialConstraint referentialConstraint, ref List<ColumnDefinition> columns, ref List<ColumnDefinition> primaryKeyColumns)
        {
            string sql = "SELECT C.name AS ColumnName, RC.name AS ReferencedColumnName"
                + " FROM sys.foreign_keys FK"
                + " JOIN sys.foreign_key_columns FKC ON FK.object_id = FKC.constraint_object_id"
                + " JOIN sys.columns C ON FKC.parent_object_id = C.object_id AND FKC.parent_column_id = C.column_id"
                + " JOIN sys.columns RC ON FKC.referenced_object_id = RC.object_id AND FKC.referenced_column_id = RC.column_id"
                + $" WHERE FK.schema_id = SCHEMA_ID('{referentialConstraint.ConstraintSchema}') AND FK.name = '{referentialConstraint.ConstraintName}'"
                + " ORDER BY FKC.constraint_column_id;";

            var dataRows = SqlUtil.GetDataRows(_connectionString, sql);

            columns = columns.OrderBy(_ => dataRows.Select(dr => dr["ColumnName"]).ToList().IndexOf(_.ColumnName)).ToList();

            primaryKeyColumns = primaryKeyColumns.OrderBy(_ => dataRows.Select(dr => dr["ReferencedColumnName"]).ToList().IndexOf(_.ColumnName)).ToList();
        }

        public bool ViewExists(string viewName)
        {
            using (var context = GetContext())
            {
                return context.Views.SingleOrDefault(_ => _.TableSchema == _schema && _.TableName == viewName) != null;
            }
        }

        internal bool TableExists(string tableName)
        {
            using (var context = GetContext())
            {
                return context.Tables.SingleOrDefault(_ => _.TableSchema == _schema && _.TableName == tableName) != null;
            }
        }
    }
}
