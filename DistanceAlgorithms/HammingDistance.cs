using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;

public abstract partial class DistanceAlgorithms
{
    /// <summary>
    /// Calculates the Hamming Distance between two equal length arrays.
    /// It is minimum of single element operations of update, needed to transfrom arr1 into arr2
    /// </summary>
    /// <param name="arr1"></param>
    /// <param name="arr2"></param>
    /// <returns>int representing the Hamming Distance</returns>
    protected static int HammingDistance<T>(T arr1, T arr2, int limit) where T : IList
    {
        int len1 = arr1.Count;
        int len2 = arr2.Count;

        if (len1 != len2)
        {
            throw new ArgumentException("inputs must be equal length: \r\n" + arr1.Count + ", \r\n" + arr2.Count);
            //throw new Exception("Strings must be equal length");
        }

        int distance = 0;

        for (int i = 0; i < len1; i++)
        {
            if (!arr1[i].Equals(arr2[i]) & distance < limit)
            {
                distance++;
            }
        }
        return distance;
    }

    protected static byte HammingDistanceX8(byte x, byte y, byte limit)
    {
        int xor = x ^ y;
        byte distance = 0;
        while (xor != 0 & distance < limit)
        {
            xor &= xor - 1;
            distance++;
        }
        return distance;
    }
    protected static byte HammingDistanceX16(short x, short y, byte limit)
    {
        int xor = (ushort)x ^ (ushort)y;
        byte distance = 0;
        while (xor != 0 & distance < limit)
        {
            xor &= xor - 1;
            distance++;
        }
        return distance;
    }
    protected static byte HammingDistanceX32(int x, int y, byte limit)
    {
        int xor = x ^ y;
        byte distance = 0;
        while (xor != 0 & distance < limit)
        {
            xor &= xor - 1;
            distance++;
        }
        return distance;
    }
    protected static byte HammingDistanceX64(long x, long y, byte limit)
    {
        long xor = x ^ y;
        byte distance = 0;
        while (xor != 0 & distance < limit)
        {
            xor &= xor - 1;
            distance++;
        }
        return distance;
    }
}

public partial class DistanceAlgorithmsSql : DistanceAlgorithms
{
    public static int HammingDistanceString(SqlString str1, SqlString str2, SqlInt32 limit)
    {
        return HammingDistance(str1.Value.ToList<char>(),
                               str2.Value.ToList<char>(),
                               limit.Value);
    }
    public static int HammingDistanceBytes(SqlBytes bytes1, SqlBytes bytes2, SqlInt32 limit)
    {
        return HammingDistance(bytes1.Value.ToBollArray().ToList(),
                               bytes2.Value.ToBollArray().ToList(), 
                               limit.Value);
    }

    public static byte HammingDistanceX8(SqlByte x,   SqlByte y,  SqlByte limit)
    {
        return DistanceAlgorithms.HammingDistanceX8(x.Value, y.Value, limit.Value);
    }
    public static byte HammingDistanceX16(SqlInt16 x, SqlInt16 y, SqlByte limit)
    {
        return DistanceAlgorithms.HammingDistanceX16(x.Value, y.Value, limit.Value);
    }
    public static byte HammingDistanceX32(SqlInt32 x, SqlInt32 y, SqlByte limit)
    {
        return DistanceAlgorithms.HammingDistanceX32(x.Value, y.Value, limit.Value);
    }
    public static byte HammingDistanceX64(SqlInt64 x, SqlInt64 y, SqlByte limit)
    {
        return DistanceAlgorithms.HammingDistanceX64(x.Value, y.Value, limit.Value);
    }
}