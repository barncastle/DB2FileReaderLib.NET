using System;
using System.Reflection;
using DB2FileReaderLib.NET.Attributes;

namespace DB2FileReaderLib.NET
{
    public class FieldCache<T>
    {
        public readonly FieldInfo Field;
        public readonly bool IsArray = false;
        public readonly bool IndexMapField = false;
        public readonly Action<T, object> Setter;

        public int Cardinality { get; set; } = 1;

        public FieldCache(FieldInfo field, bool indexMapField)
        {
            Field = field;
            IsArray = field.FieldType.IsArray;
            Setter = field.GetSetter<T>();
            IndexMapField = indexMapField;
            Cardinality = GetCardinality(field);
        }

        private int GetCardinality(FieldInfo field)
        {
            var attr = Attribute.GetCustomAttribute(field, typeof(CardinalityAttribute)) as CardinalityAttribute;
            return Math.Max(attr?.Count ?? 1, 1);
        }
    }
}
