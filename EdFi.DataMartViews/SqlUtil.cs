using Microsoft.EntityFrameworkCore;
using SqlServer.Core.InformationSchema;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace EdFi.DataMartViews
{
    public static class SqlUtil
    {
        public static SchemaDbContext GetContext(string connectionString)
        {
            var context = new SchemaDbContext(connectionString);

            // https://forums.asp.net/t/1864180.aspx?EF+returning+two+identical+records+when+2+different+records+are+in+the+table+
            // For example, query of KEY_COLUMN_USAGES will return identical rows without AsNoTracking().
            context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;

            return context;
        }

        public static void ExecuteCommand(string connectionString, string sql)
        {
            // Since upgrading EF Core, DROP statements are no longer allowed via context.Database.ExecuteSqlCommand,
            // so we use the traditional .NET db access classes instead.
            using (var connection = new SqlConnection(connectionString))
            {
                var command = new SqlCommand(sql, connection);
                command.Connection.Open();
                command.ExecuteNonQuery();
            }
        }


        public static List<DataRow> GetDataRows(string connectionString, string sql)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Connection.Open();

                    var reader = command.ExecuteReader();

                    var dt = new DataTable();
                    dt.Load(reader);

                    return dt.Rows.OfType<DataRow>().ToList();
                }
            }
        }
    }
}
