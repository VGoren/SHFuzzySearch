using System;
using System.Data.SqlTypes;
using System.Collections.Generic;
using System.Collections;
using Microsoft.SqlServer.Server;
using static Splitter;

public class SplitterSql
{
    public struct i_str
    {
        public int    i   { get; set; }
        public string str { get; set; }
    }

    public static string GetNGramHashMode(SqlByte nGramHashMode)
    {
        return Enum.IsDefined(typeof(nGramHashMode), (int)nGramHashMode.Value) == true ? ((nGramHashMode)nGramHashMode.Value).ToString() : null;
    }


    [SqlFunction(Name              = "f_STRING_SPLIT",
                 FillRowMethodName = "GetNStr",
                 TableDefinition   = "i   int" +
                                     "str nvarchar(max)")]
    public static IEnumerable f_STRING_SPLIT(SqlString str, SqlChars delimiter, SqlByte RemoveEmptyEntries)
    {
        List<i_str> i_strs = new List<i_str>();
        int i = 1;
        foreach (string s in str.Value.Split(delimiter.Value, (StringSplitOptions)RemoveEmptyEntries.Value))
        {
            i_strs.Add(new i_str { i = i, str = s });
            i++;
        }
        return i_strs;
    }

    [SqlFunction(Name              = "GetNGrams",
                 FillRowMethodName = "GetNStr",
                 TableDefinition   = "i     int" +
                                     "nGram nvarchar(10)")]
    public static IEnumerable GetNGrams(SqlString str, SqlByte n, SqlChars delimiter)
    {
        List<i_str> i_nGrams = new List<i_str>();
        int i = 1;
        foreach (Splitter.nGram s in str.Value.GetNGrams(n.Value, delimiter.Value, Splitter.nGramHashMode.classic))
        {
            i_nGrams.Add(new i_str { i = i, str = s.str });
            i++;
        }
        return i_nGrams;
    }
    
    public static void GetNStr(object row, out SqlInt32 i, out SqlString nGram)
    {
        i_str i_nGram = (i_str)row;
        i             = new SqlInt32(i_nGram.i);
        nGram         = new SqlString(i_nGram.str);
    }
}

