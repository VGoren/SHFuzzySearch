using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;


public static class SignatureHashSql
{
    public static byte[] GetSignatureHash(SqlString str, SqlByte nSize, SqlChars delimiter, SqlInt32 codepage, SqlByte nGramHashMode, SqlByte saltFilter, SqlInt32 hashSize)
    {
        return str.Value.GetSignatureHash(nSize.Value, delimiter.Value, codepage.Value, nGramHashMode.Value, (saltFilter.IsNull ? 0 : saltFilter.Value), hashSize.Value).ToByteArray();
    }

    public struct i_varbinary
    {
        public int    i         { get; set; }
        public byte[] varbinary { get; set; }
    }

    [SqlFunction(Name              = "SplitSignatureHash",
                 FillRowMethodName = "GetSignatureHashChunk",
                 TableDefinition   = "i      int" +
                                     "iChunk varbinary(max)")]
    public static IEnumerable SplitSignatureHash(SqlBytes hash, SqlInt32 chunkSize)
    {
       List<i_varbinary> i_iChunks = new List<i_varbinary> ();
       
       byte i = 0;
       foreach (var chunk in hash.Value.ToBollArray().Split(chunkSize.Value))
       {
            i_iChunks.Add(new i_varbinary {
                                               i         = i,
                                               varbinary = new byte[] { i }.Concat(chunk.Cast<bool>().ToArray().ToByteArray()).ToArray()
                                          });
           i++;
       }
       return i_iChunks;
    }
    public static void GetSignatureHashChunk(object row, out SqlInt32 i, out SqlBytes iChunk)
    {
        i_varbinary i_iChunk = (i_varbinary)row;
        i                    = new SqlInt32(i_iChunk.i);
        iChunk               = new SqlBytes(i_iChunk.varbinary);
    }

    [SqlFunction(Name              = "SplitSignatureHashKComb",
                 FillRowMethodName = "GetSignatureHashChunkKComb",
                 TableDefinition   = "i           int" +
                                     "iChunkKComb varbinary(max)")]
    public static IEnumerable SplitSignatureHashKComb(SqlBytes hash, SqlInt32 chunkSize, SqlByte kCombSize)
    {
        List<i_varbinary> i_iChunks      = SplitSignatureHash(hash, chunkSize).Cast<i_varbinary>().ToList();
        List<i_varbinary> i_iChunkKCombs = new List<i_varbinary>();

        int i = 1;
        foreach (int[] kComb in i_iChunks.GetIKCombs(kCombSize.Value))
        {
            List<byte> iChunkKComb = new List<byte>();
            foreach (var j in kComb)
            {
                iChunkKComb.AddRange(i_iChunks[j].varbinary);
            }
            i_iChunkKCombs.Add(new i_varbinary {i = i, varbinary = iChunkKComb.ToArray()});
            i++;
        }
        return i_iChunkKCombs;
    }

    public static void GetSignatureHashChunkKComb(object row, out SqlInt32 i, out SqlBytes iChunkKComb)
    {
        i_varbinary i_iChunkKComb = (i_varbinary)row;
        i                         = new SqlInt32(i_iChunkKComb.i);
        iChunkKComb               = new SqlBytes(i_iChunkKComb.varbinary);

    }
}
