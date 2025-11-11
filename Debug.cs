using System;
using System.Data.SqlTypes;
using System.Linq;
using static Splitter;

public partial class Debug
{
    public static int xor32(SqlInt32 x, SqlInt32 y)
    {
        int xor = x.Value ^ y.Value;
        return xor;
    }
}

namespace ConsoleApp
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var v101 = SignatureHashSql.GetSignatureHash                        (
                                                                                 /*str               =*/ new SqlString("Josue%Heath%Abbott"),
                                                                                 /*nSize             =*/ new SqlByte(1),
                                                                                 /*delimiter         =*/ new SqlChars(new char[] { '%' }),
                                                                                 /*codepage          =*/ new SqlInt32(1251),
                                                                                 /*nGramHashMode     =*/ new SqlByte((byte)nGramHashMode.salt_i_word),
                                                                                 /*saltFilter        =*/ SqlByte.Null,
                                                                                                         //new SqlByte(2),
                                                                                 /*hashSize          =*/ new SqlInt32(32)
                                                                                );
            var v102 = SignatureHashSql.GetSignatureHash                        (
                                                                                 /*str               =*/ new SqlString("Hosie%Heath%Abbott"),
                                                                                 /*nSize             =*/ new SqlByte(1),
                                                                                 /*delimiter         =*/ new SqlChars(new char[] { '%' }),
                                                                                 /*codepage          =*/ new SqlInt32(1251),
                                                                                 /*nGramHashMode     =*/ new SqlByte((byte)nGramHashMode.salt_i_word),
                                                                                 /*saltFilter        =*/ SqlByte.Null,
                                                                                                         //new SqlByte(2),
                                                                                 /*hashSize          =*/ new SqlInt32(32)
                                                                                );

            var v103 = BitConverter.GetBytes(v101.BytesToInt() ^ v102.BytesToInt());

            Console.WriteLine(string.Join("", v101.ToBollArray().Select(b => b ? "1" : "0").ToArray()));
            Console.WriteLine(string.Join("", v102.ToBollArray().Select(b => b ? "1" : "0").ToArray()));
            Console.WriteLine(string.Join("", v103.ToBollArray().Select(b => b ? "1" : "0").ToArray()));


            var v200  = SignatureHashSql.SplitSignatureHash                     (
                                                                                 /*hash               =*/ new SqlBytes(new byte[] { 124, 59, 45, 24 }),
                                                                                 /*chunkSize          =*/ new SqlByte(8)
                                                                                );                    
                                                                                                      
            var v300  = SignatureHashSql.SplitSignatureHashKComb                (                     
                                                                                 /*hash               =*/ new SqlBytes(new byte[] { 124, 59, 45, 24 }),
                                                                                 /*chunkSize          =*/ new SqlByte(8),
                                                                                 /*kComb              =*/ new SqlByte(2)
                                                                                );                    
                                                                                                      
            var v400  = //DistanceAlgorithmsSql.LevenshteinDistanceString       (                     
                        //DistanceAlgorithmsSql.DamerauLevenshteinDistanceString(                     
                        DistanceAlgorithmsSql.HammingDistanceString             (                     
                                                                                 /*str1               =*/ new SqlString("Lorem ipsum dolor sit amet"),
                                                                                 /*str2               =*/ new SqlString("Lorem ipsum dolor sEt amet"),
                                                                                 /*limit              =*/ new SqlInt32(2147483647)
                                                                                );
                      
            var v500  = //DistanceAlgorithmsSql.LevenshteinDistanceBytes        (
                        //DistanceAlgorithmsSql.DamerauLevenshteinDistanceBytes (
                        DistanceAlgorithmsSql.HammingDistanceBytes              (
                                                                                 /*bytes1             =*/ new SqlBytes(       "00011001".Select(el => el == '1').ToArray().ToByteArray()),
                                                                                 /*bytes2             =*/ new SqlBytes(       "00011000".Select(el => el == '1').ToArray().ToByteArray()),
                                                                                 /*limit              =*/ new SqlInt32(2147483647)
                                                                                );
                        
            var v600  = DistanceAlgorithmsSql.HammingDistanceX8                 (
                                                                                 /*x                  =*/ new SqlByte ((byte) "00011001".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*y                  =*/ new SqlByte ((byte) "00011000".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*limit              =*/ new SqlByte(2)
                                                                                );
                      
            var v700  = DistanceAlgorithmsSql.HammingDistanceX16                (
                                                                                 /*x                  =*/ new SqlInt16((short)"11111111111111111111111111111111".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*y                  =*/ new SqlInt16((short)"00011000010101010000000000000000".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*limit              =*/ new SqlByte(2)
                                                                                );
                                                                                
            var v800  = DistanceAlgorithmsSql.HammingDistanceX32                (
                                                                                 /*x                  =*/ new SqlInt32(       "11011001".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*y                  =*/ new SqlInt32(       "00011000".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*limit              =*/ new SqlByte(2)
                                                                                );
                                                                                
            var v900  = DistanceAlgorithmsSql.HammingDistanceX64                (
                                                                                 /*x                  =*/ new SqlInt64((long) "00011001".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*y                  =*/ new SqlInt64((long) "00011000".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*limit              =*/ new SqlByte(2)
                                                                                );
            var v1000 = SplitterSql.GetNGrams                                   (
                                                                                 /*str                =*/ new SqlString("Lorem ipsum dolor sit amet"),
                                                                                 /*n                  =*/ new SqlByte(1),
                                                                                 /*delimiter          =*/ new SqlChars(new char[] { '%' })
                                                                                );
                                                                                
            var v1100 = SplitterSql.GetNGramHashMode                            (
                                                                                 /*nGramHashMode      =*/ new SqlByte(3)
                                                                                );
                                                                                
            var v1200 = SplitterSql.f_STRING_SPLIT                              (
                                                                                 /*str                =*/ new SqlString("Lorem ipsum dolor sit amet"),
                                                                                 /*delimiter          =*/ new SqlChars(new char[] { '%' }),
                                                                                 /*RemoveEmptyEntries =*/ new SqlByte((byte)StringSplitOptions.RemoveEmptyEntries)
                                                                                );
                                                                               
            var v1300 = Debug.xor32                                             (
                                                                                 /*x                  =*/ new SqlInt32("00011001".Select(el => el == '1').ToArray().ToByteArray().BytesToInt()),
                                                                                 /*y                  =*/ new SqlInt32("00011000".Select(el => el == '1').ToArray().ToByteArray().BytesToInt())
                                                                                );

        }
    }
}
