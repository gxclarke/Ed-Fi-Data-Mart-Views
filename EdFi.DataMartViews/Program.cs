using Microsoft.Extensions.Configuration;
using SqlServer.Core.InformationSchema;
using System;
using System.Linq;

namespace EdFi.DataMartViews
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

            var builder = new Builder(config["connectionString"], config["schema"], config["dataMartSchemaOwner"]);

            var dataMarts = config.GetSection("dataMarts").GetChildren();

            foreach (var dataMart in dataMarts)
            {
                builder.RecreateDataMart(dataMart["schema"], dataMart.GetSection("factSourceTables").Get<string[]>());
            }
        }
    }
}
