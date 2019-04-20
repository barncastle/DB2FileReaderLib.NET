using System;
using System.Reflection;

namespace DB2FileReaderLib.NET
{
    public class FieldCache<T>
    {
        public FieldInfo Field;
        public bool IsArray = false;
        public bool IndexMapField = false;
        public int Cardinality = 1;

        public Action<T, object> Setter;

        public FieldCache(FieldInfo field, bool isArray, Action<T, object> setter, bool indexMapField, int cardinality)
        {
            this.Field = field;
            this.IsArray = isArray;
            this.Setter = setter;
            this.IndexMapField = indexMapField;
            this.Cardinality = Math.Max(cardinality, 1);
        }
    }
}
