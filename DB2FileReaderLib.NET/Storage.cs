using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using DB2FileReaderLib.NET.Attributes;

namespace DB2FileReaderLib.NET
{
    public class Storage<T> : SortedDictionary<int, T> where T : class, new()
    {
        public Storage(string fileName) : this(File.OpenRead(fileName)) { }

        public Storage(Stream stream)
        {
            DB2Reader reader = DB2Reader.FromStream(stream);

            FieldInfo[] fields = typeof(T).GetFields();

            FieldCache<T>[] fieldCache = new FieldCache<T>[fields.Length];
            for (int i = 0; i < fields.Length; ++i)
            {
                bool indexMapAttribute = reader.Flags.HasFlagExt(DB2Flags.Index) ? Attribute.IsDefined(fields[i], typeof(IndexAttribute)) : false;
                fieldCache[i] = new FieldCache<T>(fields[i], indexMapAttribute);
            }

            Parallel.ForEach(reader.AsEnumerable(), new ParallelOptions() { MaxDegreeOfParallelism = 1 }, row =>
            {
                T entry = new T();
                row.Value.GetFields(fieldCache, entry);
                lock (this)
                    Add(row.Value.Id, entry);
            });
        }
    }
}
