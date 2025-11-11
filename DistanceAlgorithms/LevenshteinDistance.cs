using System;
using System.Collections;
using System.Data.SqlTypes;
using System.Linq;

public abstract partial class DistanceAlgorithms
{
    /// <summary>
    /// Calculates the Levenshtein Distance between two arrays.
    /// It is minimum of single element operations of insert/delete/update, needed to transfrom arr1 into arr2
    /// https://pawlowski.cz/2010/12/28/sql_server-fuzzy-strings-matching-USING-levenshtein-ALGORITHM-t-SQL-vs-clr/
    /// </summary>
    /// <param name="arr1"></param>
    /// <param name="arr2"></param>
    /// <returns>int representing the Levenshtein Distance</returns>
    protected static int LevenshteinDistance<T>(T arr1, T arr2) where T : IList
    {
        int len1 = arr1.Count;
        int len2 = arr2.Count;
        int[,] d = new int[len1 + 1, len2 + 1];

        for (int i = 0; i <= len1; i++)
            d[i, 0] = i;
        for (int j = 0; j <= len2; j++)
            d[0, j] = j;

        for (int i = 1; i <= len1; i++)
        {
            for (int j = 1; j <= len2; j++)
            {
                if (arr1[i - 1].Equals(arr2[j - 1]))
                {
                    d[i, j] = d[i - 1, j - 1];
                }
                else
                    d[i, j] = Math.Min(
                              Math.Min(d[i - 1, j    ] + 1,  // delete
                                       d[i    , j - 1] + 1), // insert
                                       d[i - 1, j - 1] + 1); // update
            }
        }
        return d[len1, len2];
    }
}

public partial class DistanceAlgorithmsSql : DistanceAlgorithms
{
    public static int LevenshteinDistanceString(SqlString str1, SqlString str2)
    {
        return LevenshteinDistance(str1.Value.ToList<char>(),
                                   str2.Value.ToList<char>());
    }
    public static int LevenshteinDistanceBytes(SqlBytes bytes1, SqlBytes bytes2)
    {
        return LevenshteinDistance(bytes1.Value.ToBollArray().ToList(),
                                   bytes2.Value.ToBollArray().ToList());
    }
}
