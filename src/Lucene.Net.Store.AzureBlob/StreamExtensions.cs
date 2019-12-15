using System.IO;

namespace Lucene.Net.Store
{
    internal static class StreamExtensions
    {
        internal static void FillBuffer(this Stream stream, byte[] b, int offset, int len)
        {
            do
            {
                int read = stream.Read(b, offset, len);
                offset += read;
                len -= read;
            } while (len > 0);
        }
    }
}