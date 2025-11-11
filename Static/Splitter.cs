using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

public static class Splitter
{
    /// <summary>
    /// based on https://stackoverflow.com/a/10629938
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="list"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public static IEnumerable<IEnumerable<T>> GetKCombs<T>(this IEnumerable<T> list, int length) where T : IComparable
    {
        if (length == 1) return list.Select(t => new T[] { t }.AsEnumerable());
        return GetKCombs(list, length - 1)
            .SelectMany(t => list.Where(o => o.CompareTo(t.Last()) > 0),
                (t1, t2) => t1.Concat(new T[] { t2 }));
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="list"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public static int[][] GetIKCombs<T>(this IEnumerable<T> list, int length)
    {
        return Enumerable.Range(0, list.Count()).GetKCombs(length).ToArray().Select(el => el.Cast<int>().ToArray()).ToArray();
    }

    /// <summary>
    /// splits an array into several smaller arrays https://stackoverflow.com/a/18987605
    /// </summary>
    /// <typeparam name="T">The type of the array.</typeparam>
    /// <param name="array">The array to split.</param>
    /// <param name="size">The size of the smaller arrays.</param>
    /// <returns>An array containing smaller arrays.</returns>
    public static IEnumerable<IEnumerable<T>> Split<T>(this T[] array, int size)
    {
        for (var i = 0; i < (float)array.Length / size; i++)
        {
            yield return array.Skip(i * size).Take(size);
        }
    }
    public struct nGram
    {
        public int    i      { get; set; }
        public int    i_word { get; set; }
        public string str    { get; set; }
        public int    salt   { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="str"></param>
    /// <param name="nSize"></param>
    /// <param name="delimiter"></param>
    /// <returns></returns>
    public static List<nGram> GetNGrams(this string str, int nSize, char[] delimiter, nGramHashMode mode)
    {
        List<nGram> nGrams = new List<nGram>();

        int i = 0;
        int i_word = 0;
        foreach (string s in (delimiter == null) ? new string[1] { str } : str.Split(delimiter))
        {
            for (int j = 0; j < s.Length - (nSize - 1); j++)
            {
                nGrams.Add(new nGram 
                           { 
                               i      = i, 
                               i_word = i_word, 
                               str    = s.Substring(j, nSize), 
                               salt   = 1
                           }
                          );
                i++;
            }
            i_word++;
        }

        if (mode == nGramHashMode.salt_i_word)
        {
            nGrams = nGrams.Select(el => new nGram 
                                { 
                                    i      = el.i, 
                                    i_word = el.i_word, 
                                    str    = el.str, 
                                    salt   = el.i_word + 1
                                }
                         ).ToList();
        }

        if (mode == nGramHashMode.salt_cnt_per_word)
        {
            //https://stackoverflow.com/a/3165085
            // COUNT(*) OVER (PARTITION BY str, i_word)
            nGrams = (
                      from p in nGrams
                      group p by new { p.str, p.i_word }
                      into gr
                      from g in gr
                      select new nGram
                      {
                          i      = g.i,
                          str    = g.str,
                          i_word = g.i_word,
                          salt   = gr.Count()
                      }
                     ).ToList();
        }

        if (mode == nGramHashMode.salt_cnt)
        {
            // COUNT(*) OVER (PARTITION BY str)
            nGrams = (
                  from p in nGrams
                  group p by new { p.str }
                  into gr
                  from g in gr
                  select new nGram
                  {
                      i      = g.i,
                      str    = g.str,
                      i_word = g.i_word,
                      salt   = gr.Count()
                  }
                 ).ToList();
        }

        return nGrams;
    }

    public enum nGramHashMode : int
    {
        classic,
        salt_cnt_per_word,
        salt_cnt,
        salt_i_word
    }

    /// <summary>
    /// 1.str разбивает на n-граммы размером n
    /// </summary>
    /// <param name="str"></param>
    /// <param name="nSize"></param>
    /// <param name="delimiter"></param>
    /// <param name="mode"></param>
    /// <returns>массив hash'ей от n-грамм в виде int'ов</returns>
    public static List<int> GetNGramHashes(this string str, int nSize, char[] delimiter, int codepage, nGramHashMode mode, int saltFilter)
    {
        List<int> nums = new List<int>();

        if (saltFilter == 0)
        {
            foreach (nGram nGram in str.GetNGrams(nSize, delimiter, mode)) 
            { nums.Add(new Random(Encoding.GetEncoding(codepage).GetBytes(nGram.str).BytesToInt()).Next() *                            nGram.salt      ); }
        }
        else
        {
            foreach (nGram nGram in str.GetNGrams(nSize, delimiter, mode)) 
            { nums.Add(new Random(Encoding.GetEncoding(codepage).GetBytes(nGram.str).BytesToInt()).Next() * (saltFilter == nGram.salt ? nGram.salt : 0)); }
        }

        return nums;
    }
}