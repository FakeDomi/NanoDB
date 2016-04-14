using System;
using System.Diagnostics;
using domi1819.NanoDB;

namespace TestApp
{
    public class Program
    {
        private static readonly char[] randomStringElements = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
        private static readonly Random random = new Random();

        public static void Main(string[] args)
        {
            NanoDBFile dbFile = new NanoDBFile("file.nano");

            Stopwatch watch = new Stopwatch();
            watch.Start();

            InitializeResult initResult = dbFile.Initialize();

            if (initResult == InitializeResult.Success)
            {
                dbFile.Load(dbFile.RecommendedIndex);
            }
            else
            {
                dbFile.CreateNew(new NanoDBLayout(NanoDBElement.String8, NanoDBElement.Int, NanoDBElement.Int, NanoDBElement.Bool), 0);
            }

            dbFile.Bind();

            Console.WriteLine(watch.Elapsed);

            watch.Restart();

            NanoDBLine line = dbFile.GetLine("aaaa");

            line[2] = 123456789;

            Console.WriteLine(watch.Elapsed);


            dbFile.Unbind();

            Console.ReadKey();
        }

        internal static string GetRandomString(int length)
        {
            char[] chars = new char[length];

            for (int i = 0; i < length; i++)
            {
                chars[i] = randomStringElements[random.Next(randomStringElements.Length)];
            }

            return new string(chars);
        }
    }
}
