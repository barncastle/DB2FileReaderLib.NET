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
            DB2Reader reader;

            using (stream)
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
