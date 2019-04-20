using System;

namespace DB2FileReaderLib.NET.Attributes
{
    public class CardinalityAttribute : Attribute
    {
        public readonly int Count;

        public CardinalityAttribute(int count) => Count = count;
    }
}
