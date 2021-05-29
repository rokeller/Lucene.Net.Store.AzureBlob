using System;
using System.Diagnostics;
using System.IO;
using Azure;
using Azure.Storage.Blobs.Models;

namespace Lucene.Net.Store
{
    internal sealed class CachedInput
    {
        private CachedInput(ETag eTag, RAMFile file)
        {
            ETag = eTag;
            File = file;
        }

        public ETag ETag { get; }
        public RAMFile File { get; }

        internal static CachedInput Create(string name, BlobDownloadInfo blobDownloadInfo)
        {
            return new CachedInput(
                blobDownloadInfo.Details.ETag,
                CacheFile.FromStream(blobDownloadInfo.ContentLength, blobDownloadInfo.Content));
        }

        private sealed class CacheFile : RAMFile
        {
            internal static CacheFile FromStream(long length, Stream stream)
            {
                CacheFile file = new CacheFile();
                long remaining = length;

                while (remaining > 0)
                {
                    long bufSize = Math.Min(remaining, 32 * 1024 /* 32 KB */);
                    Debug.Assert(bufSize <= 32 * 1024, "The buffer size must not exceed 32K.");
                    int intBufSize = (int)bufSize; // Cannot exceed 32K, so conversion is safe.

                    byte[] buffer = file.AddBuffer(intBufSize);
                    stream.FillBuffer(buffer, 0, intBufSize);

                    remaining -= intBufSize;
                }

                return file;
            }
        }
    }
}