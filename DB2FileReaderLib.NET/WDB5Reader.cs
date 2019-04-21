﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DB2FileReaderLib.NET
{
    public class WDB5Row : IDB2Row
    {
        private BitReader m_data;
        private DB2Reader m_reader;
        private readonly int m_dataOffset;
        private readonly int m_recordIndex;
        private readonly bool m_hasId;

        public int Id { get; set; }
        public BitReader Data { get => m_data; set => m_data = value; }

        private readonly FieldMetaData[] m_fieldMeta;

        public WDB5Row(DB2Reader reader, BitReader data, int id, int recordIndex)
        {
            m_reader = reader;
            m_data = data;
            m_recordIndex = recordIndex;

            m_dataOffset = m_data.Offset;

            m_fieldMeta = reader.Meta;

            if (id > -1)
            {
                Id = id;
                m_hasId = true;
            }
            else if(m_reader.IdFieldIndex > 0)
            {
                int idFieldIndex = reader.IdFieldIndex;
                m_data.Position = m_fieldMeta[idFieldIndex].Offset * 8;
                Id = GetFieldValue<int>(m_data, m_fieldMeta[idFieldIndex]);
                m_hasId = true;
            }
            else
            {
                Id = recordIndex + 1;
            }
        }

        private static Dictionary<Type, Func<BitReader, FieldMetaData, Dictionary<long, string>, DB2Reader, object>> simpleReaders = new Dictionary<Type, Func<BitReader, FieldMetaData, Dictionary<long, string>, DB2Reader, object>>
        {
            [typeof(long)] = (data, fieldMeta, stringTable, header) => GetFieldValue<long>(data, fieldMeta),
            [typeof(float)] = (data, fieldMeta, stringTable, header) => GetFieldValue<float>(data, fieldMeta),
            [typeof(int)] = (data, fieldMeta, stringTable, header) => GetFieldValue<int>(data, fieldMeta),
            [typeof(uint)] = (data, fieldMeta, stringTable, header) => GetFieldValue<uint>(data, fieldMeta),
            [typeof(short)] = (data, fieldMeta, stringTable, header) => GetFieldValue<short>(data, fieldMeta),
            [typeof(ushort)] = (data, fieldMeta, stringTable, header) => GetFieldValue<ushort>(data, fieldMeta),
            [typeof(sbyte)] = (data, fieldMeta, stringTable, header) => GetFieldValue<sbyte>(data, fieldMeta),
            [typeof(byte)] = (data, fieldMeta, stringTable, header) => GetFieldValue<byte>(data, fieldMeta),
            [typeof(string)] = (data, fieldMeta, stringTable, header) => stringTable[GetFieldValue<int>(data, fieldMeta)],
        };

        private static Dictionary<Type, Func<BitReader, FieldMetaData, Dictionary<long, string>, int, object>> arrayReaders = new Dictionary<Type, Func<BitReader, FieldMetaData, Dictionary<long, string>, int, object>>
        {
            [typeof(ulong[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<ulong>(data, fieldMeta, cardinality),
            [typeof(long[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<long>(data, fieldMeta, cardinality),
            [typeof(float[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<float>(data, fieldMeta, cardinality),
            [typeof(int[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<int>(data, fieldMeta, cardinality),
            [typeof(uint[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<uint>(data, fieldMeta, cardinality),
            [typeof(ulong[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<ulong>(data, fieldMeta, cardinality),
            [typeof(ushort[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<ushort>(data, fieldMeta, cardinality),
            [typeof(short[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<short>(data, fieldMeta, cardinality),
            [typeof(byte[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<byte>(data, fieldMeta, cardinality),
            [typeof(sbyte[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<sbyte>(data, fieldMeta, cardinality),
            [typeof(string[])] = (data, fieldMeta, stringTable, cardinality) => GetFieldValueArray<int>(data, fieldMeta, cardinality).Select(i => stringTable[i]).ToArray(),
        };

        public void GetFields<T>(FieldCache<T>[] fields, T entry)
        {
            int indexFieldOffSet = 0;

            for (int i = 0; i < fields.Length; ++i)
            {
                FieldCache<T> info = fields[i];
                if (info.IndexMapField)
                {
                    if(m_hasId)
                    {
                        indexFieldOffSet++;
                    }
                    else
                    {
                        m_data.Offset = m_dataOffset;
                        m_data.Position = m_fieldMeta[i].Offset * 8;
                        Id = GetFieldValue<int>(m_data, m_fieldMeta[i]);
                    }
                    
                    info.Setter(entry, Convert.ChangeType(Id, info.Field.FieldType));
                    continue;
                }

                object value = null;
                int fieldIndex = i - indexFieldOffSet;

                if (!m_reader.Flags.HasFlagExt(DB2Flags.Sparse))
                {
                    m_data.Position = m_fieldMeta[fieldIndex].Offset * 8;
                    m_data.Offset = m_dataOffset;
                }

                if (info.IsArray)
                {
                    if (info.Cardinality <= 1)
                        SetCardinality(info, fieldIndex);

                    if (arrayReaders.TryGetValue(info.Field.FieldType, out var reader))
                        value = reader(m_data, m_fieldMeta[fieldIndex], m_reader.StringTable, info.Cardinality);
                    else
                        throw new Exception("Unhandled array type: " + typeof(T).Name);
                }
                else
                {
                    if (simpleReaders.TryGetValue(info.Field.FieldType, out var reader))
                        value = reader(m_data, m_fieldMeta[fieldIndex], m_reader.StringTable, m_reader);
                    else
                        throw new Exception("Unhandled field type: " + typeof(T).Name);
                }

                info.Setter(entry, value);
            }
        }

        /// <summary>
        /// Cardinality can be calculated from the file itself, there are three criteria to account for
        /// - Sparse Table : ((sparse size + offset) of the record - current offset) / sizeof(ValueType)
        /// - Last field of the record : (header.RecordSize - current offset) / sizeof(ValueType)
        /// - Middle field : (next field offset - current offset) / sizeof(ValueType)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="info"></param>
        /// <param name="fieldIndex"></param>
        private void SetCardinality<T>(FieldCache<T> info, int fieldIndex)
        {
            int fieldOffset = m_fieldMeta[fieldIndex].Offset;
            int fieldValueSize = (32 - m_fieldMeta[fieldIndex].Bits) >> 3;

            int nextOffset;
            if (m_reader.Flags.HasFlagExt(DB2Flags.Sparse))
                nextOffset = m_reader.sparseEntries[fieldIndex].Size + fieldOffset; // get sparse size
            else if (fieldIndex + 1 >= m_fieldMeta.Length)
                nextOffset = m_reader.RecordSize; // get total record size
            else
                nextOffset = m_fieldMeta[fieldIndex + 1].Offset; // get next field offset

            info.Cardinality = (nextOffset - fieldOffset) / fieldValueSize;
        }

        private static T GetFieldValue<T>(BitReader r, FieldMetaData fieldMeta) where T : struct
        {
            return r.ReadValue64(32 - fieldMeta.Bits).GetValue<T>();
        }

        private static T[] GetFieldValueArray<T>(BitReader r, FieldMetaData fieldMeta, int cardinality) where T : struct
        {
            T[] arr1 = new T[cardinality];
            for (int i = 0; i < arr1.Length; i++)
                arr1[i] = r.ReadValue64(32 - fieldMeta.Bits).GetValue<T>();

            return arr1;
        }

        public IDB2Row Clone()
        {
            return (IDB2Row)MemberwiseClone();
        }
    }

    public class WDB5Reader : DB2Reader
    {
        private const int HeaderSize = 52;
        private const uint WDB5FmtSig = 0x35424457; // WDB5

        public WDB5Reader(string dbcFile) : this(new FileStream(dbcFile, FileMode.Open)) { }

        public WDB5Reader(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.UTF8))
            {
                if (reader.BaseStream.Length < HeaderSize)
                    throw new InvalidDataException(string.Format("WDB5 file is corrupted!"));

                uint magic = reader.ReadUInt32();

                if (magic != WDB5FmtSig)
                    throw new InvalidDataException(string.Format("WDB5 file is corrupted!"));

                RecordsCount = reader.ReadInt32();
                FieldsCount = reader.ReadInt32();
                RecordSize = reader.ReadInt32();
                StringTableSize = reader.ReadInt32();
                TableHash = reader.ReadUInt32();
                LayoutHash = reader.ReadUInt32();
                MinIndex = reader.ReadInt32();
                MaxIndex = reader.ReadInt32();
                int locale = reader.ReadInt32();
                int copyTableSize = reader.ReadInt32();
                Flags = (DB2Flags)reader.ReadUInt16();
                IdFieldIndex = reader.ReadUInt16();

                if (RecordsCount == 0)
                    return;

                // field meta data
                m_meta = reader.ReadArray<FieldMetaData>(FieldsCount);

                if (!Flags.HasFlagExt(DB2Flags.Sparse))
                {
                    // records data
                    recordsData = reader.ReadBytes(RecordsCount * RecordSize);
                    Array.Resize(ref recordsData, recordsData.Length + 8); // pad with extra zeros so we don't crash when reading

                    // string table
                    m_stringsTable = new Dictionary<long, string>();
                    for (int i = 0; i < StringTableSize;)
                    {
                        long oldPos = reader.BaseStream.Position;

                        m_stringsTable[i] = reader.ReadCString();

                        i += (int)(reader.BaseStream.Position - oldPos);
                    }
                }
                else
                {
                    // sparse data with inlined strings
                    recordsData = reader.ReadBytes(StringTableSize - (int)reader.BaseStream.Position);

                    var tempSparseEntries = new Dictionary<uint, SparseEntry>();
                    for (int i = 0; i < (MaxIndex - MinIndex + 1); i++)
                    {
                        SparseEntry sparse = reader.Read<SparseEntry>();
                        if (sparse.Offset == 0 || sparse.Size == 0)
                            continue;

                        tempSparseEntries[sparse.Offset] = sparse;
                    }

                    sparseEntries = tempSparseEntries.Values.ToArray();
                }

                // secondary key
                if (Flags.HasFlagExt(DB2Flags.SecondaryKey))
                    reader.BaseStream.Position += (MaxIndex - MinIndex + 1) * 4;

                // index table
                if (Flags.HasFlagExt(DB2Flags.Index))
                    m_indexData = reader.ReadArray<int>(RecordsCount);

                // duplicate rows data
                Dictionary<int, int> copyData = new Dictionary<int, int>();
                for (int i = 0; i < copyTableSize / 8; i++)
                    copyData[reader.ReadInt32()] = reader.ReadInt32();

                int position = 0;
                for (int i = 0; i < RecordsCount; ++i)
                {
                    BitReader bitReader = new BitReader(recordsData) { Position = 0 };

                    if (Flags.HasFlagExt(DB2Flags.Sparse))
                    {
                        bitReader.Position = position;
                        position += sparseEntries[i].Size * 8;
                    }
                    else
                    {
                        bitReader.Offset = i * RecordSize;
                    }

                    IDB2Row rec = new WDB5Row(this, bitReader, Flags.HasFlagExt(DB2Flags.Index) ? m_indexData[i] : -1, i);
                    _Records.Add(rec.Id, rec);
                }

                foreach (var copyRow in copyData)
                {
                    IDB2Row rec = _Records[copyRow.Value].Clone();
                    rec.Data = new BitReader(recordsData);

                    rec.Data.Position = Flags.HasFlagExt(DB2Flags.Sparse) ? _Records[copyRow.Value].Data.Position : 0;
                    rec.Data.Offset = Flags.HasFlagExt(DB2Flags.Sparse) ? 0 : _Records[copyRow.Value].Data.Offset;

                    rec.Id = copyRow.Key;
                    _Records.Add(copyRow.Key, rec);
                }

                // HACK prior to 21737 this was always 0 filled
                if (IdFieldIndex == 0)
                    Flags |= DB2Flags.Index;
            }
        }
    }
}
