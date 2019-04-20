using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using DB2FileReaderLib.NET.Attributes;

namespace DB2FileReaderLib.NET
{
    public class Storage<T> : IReadOnlyDictionary<int, T> where T : class, new()
    {
        private SortedDictionary<int, T> Dictionary;

        public Storage(string fileName)
        {
            DB2Reader reader;

            using (var stream = File.OpenRead(fileName))
            using (var bin = new BinaryReader(stream))
            {
                var identifier = new string(bin.ReadChars(4));
                stream.Position = 0;
                switch (identifier)
                {
                    case "WDC3":
                        reader = new WDC3Reader(stream);
                        break;
                    case "WDC2":
                    case "1SLC":
                        reader = new WDC2Reader(stream);
                        break;
                    case "WDC1":
                        reader = new WDC1Reader(stream);
                        break;
                    case "WDB6":
                        reader = new WDB6Reader(stream);
                        break;
                    case "WDB5":
                        reader = new WDB5Reader(stream);
                        break;
                    case "WDB4":
                        reader = new WDB4Reader(stream);
                        break;
                    case "WDB3":
                        reader = new WDB3Reader(stream);
                        break;
                    case "WDB2":
                        reader = new WDB2Reader(stream);
                        break;
                    case "WDBC":
                        reader = new WDBCReader(stream);
                        break;
                    default:
                        throw new Exception("DB type " + identifier + " is not supported!");
                }
            }

            FieldInfo[] fields = typeof(T).GetFields();

            FieldCache<T>[] fieldCache = new FieldCache<T>[fields.Length];

            for (int i = 0; i < fields.Length; ++i)
            {
                bool indexMapAttribute = reader.Flags.HasFlagExt(DB2Flags.Index) ? Attribute.IsDefined(fields[i], typeof(IndexAttribute)) : false;
                int cardinality = (Attribute.GetCustomAttribute(fields[i], typeof(CardinalityAttribute)) as CardinalityAttribute)?.Count ?? 1;

                fieldCache[i] = new FieldCache<T>(fields[i], fields[i].FieldType.IsArray, fields[i].GetSetter<T>(), indexMapAttribute, cardinality);
            }

            var temp = new ConcurrentDictionary<int, T>(Environment.ProcessorCount, reader.RecordsCount);
            Parallel.ForEach(reader.AsEnumerable(), row =>
            {
                T entry = new T();
                row.Value.GetFields(fieldCache, entry);
                temp.TryAdd(row.Value.Id, entry);
            });

            Dictionary = new SortedDictionary<int, T>(temp);
        }


        #region Interface

        public T this[int key] => Dictionary[key];

        public IEnumerable<int> Keys => Dictionary.Keys;

        public IEnumerable<T> Values => Dictionary.Values;

        public int Count => Dictionary.Count;

        public bool ContainsKey(int key) => Dictionary.ContainsKey(key);

        public IEnumerator<KeyValuePair<int, T>> GetEnumerator() => Dictionary.GetEnumerator();

        public bool TryGetValue(int key, out T value) => Dictionary.TryGetValue(key, out value);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        #endregion
    }
}
