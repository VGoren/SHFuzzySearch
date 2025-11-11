using System;
using System.Collections.Generic;

public static class Converter
{
    /// <summary>
    /// https://stackoverflow.com/a/3436456
    /// </summary>
    /// <param name="str">"1010010"</param>
    /// <returns></returns>
    public static byte[] BinaryStringToBytes(string str)
    {
        byte[] bytes = new byte[str.Length / 8];
        for (int i = 0; i < bytes.Length; ++i)
        {
            bytes[i] = Convert.ToByte(str.Substring(8 * i, 8), 2);
        }
        return bytes;
    }

    /// <summary>
    /// based on https://stackoverflow.com/a/6165178
    /// </summary>
    /// <param name="bytes"></param>
    /// <returns></returns>
    public static int BytesToInt(this byte[] bytes)
    {
        if (bytes.Length > 4)
        {
            /*
            throw new ArgumentException("\r\n" + "bytes.Length > 4" + "\r\n" +
                                                 "bytes.Length = " + bytes.Length
                                       );
            */
            return System.Text.Encoding.UTF8.GetString(bytes, 0, bytes.Length).GetHashCode();
        }

        int pos = 0;
        int result = 0;
        foreach (byte b in bytes)
        {
            result |= b << pos;
            pos += 8;
        }
        return result;
    }

    /// <summary>
    /// bool[] -> byte[]
    /// based on https://stackoverflow.com/a/8987189
    /// able to return incompletely filled bytes
    /// </summary>
    /// <param name="input"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static byte[] ToByteArray(this bool[] input)
    {
        if (input.Length <= 8)
        {
            Array.Resize(ref input, 8);
        }

        if (input.Length % 8 != 0)
        {
            throw new ArgumentException("\r\n" + "input.Length % 8 != 0" + "\r\n" +
                                                 "input.Length = " + input.Length
                                       );
        }
        byte[] ret = new byte[input.Length / 8];
        for (int i = 0; i < input.Length; i += 8)
        {
            int value = 0;
            for (int j = 0; j < 8; j++)
            {
                if (input[i + j])
                {
                    value += 1 << (7 - j);
                }
            }
            ret[i / 8] = (byte)value;
        }
        return ret;
    }

    /// <summary>
    /// based on https://www.cyberforum.ru/csharp-beginners/thread411786.html
    /// </summary>
    /// <param name="input"></param>
    /// <returns></returns>
    public static bool[] ToBollArray(this byte[] input)
    {
        bool[] bits = new bool[input.Length * 8];

        for (int i = 0; i < input.Length; i++)
        {
            for (int j = 0; j < 8; j++)
                bits[i * 8 + j] = (input[i] & (1 << 7 - j)) != 0;
        }
        return bits;
    }

    /// <summary>
    /// https://stackoverflow.com/a/2548060
    /// </summary>
    /// <param name="b"></param>
    /// <returns></returns>
    public static IEnumerable<bool> GetBits(byte b)
    {
        for (int i = 0; i < 8; i++)
        {
            yield return (b & 0x80) != 0;
            b *= 2;
        }
    }
}