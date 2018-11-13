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
        string _schema;
        string _lookupSchema;
        string _dataMartSchemaOwner;

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
        }

        public void RecreateDataMart(string viewSchema, string[] factSourceTables)
        {
            CreateSchema(viewSchema);
            DropExistingViews(viewSchema);

            var viewCreator = new ViewCreator(_connectionString, _schema, viewSchema, _lookupSchema);

            foreach (string factSourceTable in factSourceTables)
                viewCreator.ProcessFactTable(factSourceTable);
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
    }
}
