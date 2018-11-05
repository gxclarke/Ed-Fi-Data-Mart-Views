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
        string _dataMartSchemaOwner;

        public Builder(string connectionString, string schema, string dataMartSchemaOwner)
        {
            _connectionString = connectionString;
            _schema = schema;
            _dataMartSchemaOwner = dataMartSchemaOwner;
        }

        public void RecreateDataMart(string viewSchema, string[] factSourceTables)
        {
            using (var context = new SchemaDbContext(_connectionString))
            {
                if (context.Schemata.SingleOrDefault(_ => _.SchemaName == viewSchema) == null)
                {
                    string sql = $"CREATE SCHEMA [{viewSchema}] AUTHORIZATION [{_dataMartSchemaOwner}];";
                    SqlUtil.ExecuteCommand(_connectionString, sql);
                }
            }
            DropExistingViews(viewSchema);

            var viewCreator = new ViewCreator(_connectionString, _schema, viewSchema);

            foreach (string factSourceTable in factSourceTables)
                viewCreator.ProcessFactTable(factSourceTable);
        }

        private void DropExistingViews(string viewSchema)
        {
            using (var context = new SchemaDbContext(_connectionString))
            {
                foreach (var view in context.Views.Where(_ => _.TableSchema == viewSchema).ToList())
                {
                    string sql = "DROP VIEW [" + view.TableSchema + "].[" + view.TableName + "];";
                    SqlUtil.ExecuteCommand(_connectionString, sql);
                }
            }
        }
    }
}
