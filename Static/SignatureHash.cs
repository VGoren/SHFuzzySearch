using System.Collections.Generic;
using static Splitter;

public static class SignatureHash
{
    /// <summary>
    /// P.S. bool[] faster thane BitArray, but more expensive for memory https://stackoverflow.com/a/33500328
    /// </summary>
    /// <param name="str"></param>
    /// <param name="n"></param>
    /// <param name="hashSize"></param>
    /// <returns></returns>
    private static bool[] GetSignatureHash(this List<int> hashes, int hashSize)
    {
        bool[] bitmask = new bool[hashSize];

        foreach (int hash in hashes)
        {
            bitmask[(uint)hash % (uint)hashSize] = true;
        }

        return bitmask;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="str"></param>
    /// <param name="nSize"></param>
    /// <param name="delimiter"></param>
    /// <param name="nGramHashMode"></param>
    /// <param name="hashSize"></param>
    /// <returns></returns>
    public static bool[] GetSignatureHash(this string str, int nSize, char[] delimiter, int codepage, int nGramHashMode, int saltFilter, int hashSize)
    {
        return str.GetNGramHashes(nSize, delimiter, codepage, (nGramHashMode)nGramHashMode, saltFilter).GetSignatureHash(hashSize);
    }
}
