using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DB2FileReaderLib.NET
{
    public class WDBCRow : IDB2Row
    {
        private BitReader m_data;
        private DB2Reader m_reader;
        private readonly int m_recordIndex;

        public int Id { get; set; }
        public BitReader Data { get => m_data; set => m_data = value; }

        public WDBCRow(DB2Reader reader, BitReader data, int recordIndex)
        {
            m_reader = reader;
            m_data = data;
            m_recordIndex = recordIndex;

            Id = recordIndex + 1; // this gets overriden on GetFields<>
        }

        private static Dictionary<Type, Func<BitReader, Dictionary<long, string>, DB2Reader, object>> simpleReaders = new Dictionary<Type, Func<BitReader, Dictionary<long, string>, DB2Reader, object>>
        {
            [typeof(long)] = (data, stringTable, header) => GetFieldValue<long>(data),
            [typeof(float)] = (data, stringTable, header) => GetFieldValue<float>(data),
            [typeof(int)] = (data, stringTable, header) => GetFieldValue<int>(data),
            [typeof(uint)] = (data, stringTable, header) => GetFieldValue<uint>(data),
            [typeof(short)] = (data, stringTable, header) => GetFieldValue<short>(data),
            [typeof(ushort)] = (data, stringTable, header) => GetFieldValue<ushort>(data),
            [typeof(sbyte)] = (data, stringTable, header) => GetFieldValue<sbyte>(data),
            [typeof(byte)] = (data, stringTable, header) => GetFieldValue<byte>(data),
            [typeof(string)] = (data, stringTable, header) => stringTable[GetFieldValue<int>(data)],
        };

        private static Dictionary<Type, Func<BitReader, Dictionary<long, string>, int, object>> arrayReaders = new Dictionary<Type, Func<BitReader, Dictionary<long, string>, int, object>>
        {
            [typeof(ulong[])] = (data, stringTable, cardinality) => GetFieldValueArray<ulong>(data, cardinality),
            [typeof(long[])] = (data, stringTable, cardinality) => GetFieldValueArray<long>(data, cardinality),
            [typeof(float[])] = (data, stringTable, cardinality) => GetFieldValueArray<float>(data, cardinality),
            [typeof(int[])] = (data, stringTable, cardinality) => GetFieldValueArray<int>(data, cardinality),
            [typeof(uint[])] = (data, stringTable, cardinality) => GetFieldValueArray<uint>(data, cardinality),
            [typeof(ulong[])] = (data, stringTable, cardinality) => GetFieldValueArray<ulong>(data, cardinality),
            [typeof(ushort[])] = (data, stringTable, cardinality) => GetFieldValueArray<ushort>(data, cardinality),
            [typeof(short[])] = (data, stringTable, cardinality) => GetFieldValueArray<short>(data, cardinality),
            [typeof(byte[])] = (data, stringTable, cardinality) => GetFieldValueArray<byte>(data, cardinality),
            [typeof(sbyte[])] = (data, stringTable, cardinality) => GetFieldValueArray<sbyte>(data, cardinality),
            [typeof(string[])] = (data, stringTable, cardinality) => GetFieldValueArray<int>(data, cardinality).Select(i => stringTable[i]).ToArray(),
        };

        public void GetFields<T>(FieldCache<T>[] fields, T entry)
        {
            for (int i = 0; i < fields.Length; ++i)
            {
                FieldCache<T> info = fields[i];
                if (info.IndexMapField)
                {
                    // set the real id
                    Id = GetFieldValue<int>(m_data);

                    info.Setter(entry, Convert.ChangeType(Id, info.Field.FieldType));
                    continue;
                }

                object value = null;

                if (info.IsArray)
                {
                    if (arrayReaders.TryGetValue(info.Field.FieldType, out var reader))
                        value = reader(m_data, m_reader.StringTable, info.Cardinality);
                    else
                        throw new Exception("Unhandled array type: " + typeof(T).Name);
                }
                else
                {
                    if (simpleReaders.TryGetValue(info.Field.FieldType, out var reader))
                        value = reader(m_data, m_reader.StringTable, m_reader);
                    else
                        throw new Exception("Unhandled field type: " + typeof(T).Name);
                }

                info.Setter(entry, value);
            }
        }

        private static T GetFieldValue<T>(BitReader r) where T : struct
        {
            return r.ReadValue64(FastStruct<T>.Size * 8).GetValue<T>();
        }

        private static T[] GetFieldValueArray<T>(BitReader r, int cardinality) where T : struct
        {
            T[] arr1 = new T[cardinality];
            for (int i = 0; i < arr1.Length; i++)
                arr1[i] = r.ReadValue64(FastStruct<T>.Size * 8).GetValue<T>();

            return arr1;
        }

        public IDB2Row Clone()
        {
            return (IDB2Row)MemberwiseClone();
        }
    }

    public class WDBCReader : DB2Reader
    {
        private const int HeaderSize = 20;
        private const uint WDBCFmtSig = 0x43424457; // WDBC

        public WDBCReader(string dbcFile) : this(new FileStream(dbcFile, FileMode.Open)) { }

        public WDBCReader(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.UTF8))
            {
                if (reader.BaseStream.Length < HeaderSize)
                    throw new InvalidDataException(string.Format("WDBC file is corrupted!"));

                uint magic = reader.ReadUInt32();

                if (magic != WDBCFmtSig)
                    throw new InvalidDataException(string.Format("WDBC file is corrupted!"));

                Flags |= DB2Flags.Index; // HACK

                RecordsCount = reader.ReadInt32();
                FieldsCount = reader.ReadInt32();
                RecordSize = reader.ReadInt32();
                StringTableSize = reader.ReadInt32();

                if (RecordsCount == 0)
                    return;

                recordsData = reader.ReadBytes(RecordsCount * RecordSize);
                Array.Resize(ref recordsData, recordsData.Length + 8); // pad with extra zeros so we don't crash when reading

                for (int i = 0; i < RecordsCount; i++)
                {
                    BitReader bitReader = new BitReader(recordsData) { Position = i * RecordSize * 8 };
                    IDB2Row rec = new WDBCRow(this, bitReader, i);

                    // HACK WDBC and WDB2 may not have an index 
                    // so temporaryily use the ordinal and replace with the real id
                    // if IndexAttribute exists on GetFields<>
                    _Records.Add(i, rec);
                }

                m_stringsTable = new Dictionary<long, string>();

                for (int i = 0; i < StringTableSize;)
                {
                    long oldPos = reader.BaseStream.Position;

                    m_stringsTable[i] = reader.ReadCString();

                    i += (int)(reader.BaseStream.Position - oldPos);
                }
            }
        }
    }
}
