using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static EdFi.DataMartViews.SchemaAnalyzer;

namespace EdFi.DataMartViews
{
    public class ViewDefinition
    {
        public class ColumnGroupDefinition
        {
            public List<ColumnDefinition> Columns { get; set; }

            public ColumnGroupDefinition()
            {
                Columns = new List<ColumnDefinition>();
            }
        }

        public class PrimaryKey : ColumnGroupDefinition
        {
        }

        public enum ReferenceKeyType
        {
            DimensionReference,
            LookupReference,
            Denormalized
        }

        public class ReferenceKey: ColumnGroupDefinition
        {
            public ForeignKeyRelationship Relationship { get; set; }
            public ReferenceKeyType KeyType { get; set; }
        }

        public string Name { get; set; }
        public ViewType Type { get; set; }

        public List<ColumnGroupDefinition> ColumnGroups { get; set; }

        public ViewDefinition(string name, ViewType type)
        {
            Name = name;
            Type = type;
            ColumnGroups = new List<ColumnGroupDefinition>();
        }

        public IEnumerable<string> GetPrimaryKeyTables()
        {
            return ColumnGroups.OfType<ReferenceKey>().Select(_ => _.Relationship.PrimaryKeyTable);
        }
    }
}
