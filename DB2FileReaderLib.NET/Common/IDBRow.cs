using System;
using System.Collections.Generic;
using System.Text;

namespace DB2FileReaderLib.NET.Common
{
    interface IDBRow
    {
        int Id { get; set; }
        BitReader Data { get; set; }
        void GetFields<T>(FieldCache<T>[] fields, T entry);
        IDBRow Clone();
    }
}
