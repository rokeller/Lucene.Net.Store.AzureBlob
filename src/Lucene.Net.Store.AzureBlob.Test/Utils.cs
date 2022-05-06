using System;
using System.IO;
using System.Text;
using Azure.Storage.Blobs;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Util;

namespace Lucene.Net.Store
{
    public static class Utils
    {
        private static readonly string RandomStringChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklrmopqrstuvwxyz0123456789";

        public static readonly Random Rng = new Random();

        public static readonly LuceneVersion Version = Lucene.Net.Util.LuceneVersion.LUCENE_48;

        public static readonly Analyzer StandardAnalyzer = new StandardAnalyzer(Version);

        public static readonly string StorageConnectionString = Environment.GetEnvironmentVariable("CONNECTION_STRING") ?? "UseDevelopmentStorage=true";

        public static BlobServiceClient GetBlobServiceClient()
        {
            return new BlobServiceClient(StorageConnectionString);
        }

        public static BlobContainerClient GetBlobContainerClient(string containerName)
        {
            return GetBlobServiceClient().GetBlobContainerClient(containerName);
        }

        public static string GenerateRandomString(int len)
        {
            StringBuilder sb = new StringBuilder(len);

            for (int i = 0; i < len; i++)
            {
                sb.Append(RandomStringChars[Rng.Next(RandomStringChars.Length)]);
            }

            return sb.ToString();
        }

        public static byte[] GenerateRandomBuffer(int len)
        {
            byte[] buffer = new byte[len];

            Rng.NextBytes(buffer);

            return buffer;
        }

        public static int GenerateRandomInt(int max)
        {
            return GenerateRandomInt(0, max);
        }

        public static int GenerateRandomInt(int min, int max)
        {
            return Rng.Next(min, max);
        }

        public static void WriteRepeatedly(Stream target, int len, string text)
        {
            Span<byte> buffer = stackalloc byte[Encoding.ASCII.GetByteCount(text)];
            Encoding.ASCII.GetBytes(text, buffer);
            int remaining = len;

            while (remaining > 0)
            {
                int toWrite = Math.Min(buffer.Length, remaining);
                target.Write(buffer.Slice(0, toWrite));
                remaining -= toWrite;
            }
        }
    }
}
