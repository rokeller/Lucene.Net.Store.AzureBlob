using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Lucene.Net.Index;

namespace Lucene.Net.Store
{
    public class AzureBlobDirectory : AzureBlobDirectoryBase
    {
        private readonly ConcurrentDictionary<string, CachedInput> cachedInputs = new ConcurrentDictionary<string, CachedInput>(StringComparer.Ordinal);

        public AzureBlobDirectory(BlobContainerClient blobContainerClient, string blobPrefix) : this(blobContainerClient, blobPrefix, null)
        { }

        public AzureBlobDirectory(BlobContainerClient blobContainerClient, string blobPrefix, AzureBlobDirectoryOptions options)
        : base(blobContainerClient, blobPrefix, options)
        { }

        #region Directory Implementation

        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            EnsureOpen();

            return new AzureBlobIndexOutput(GetBlockBlobClient(name));
        }

        public override void DeleteFile(string name)
        {
            EnsureOpen();

            DeleteBlob(name);
        }

        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name)
        {
            EnsureOpen();

            return BlobExists(name);
        }

        public override long FileLength(string name)
        {
            EnsureOpen();

            return GetBlobLength(name);
        }

        public override string[] ListAll()
        {
            EnsureOpen();

            string[] names = ListBlobs().Select(ExtractBlobName).ToArray();

            return names;
        }

        public override IndexInput OpenInput(string name, IOContext context)
        {
            EnsureOpen();

            return GetIndexInput(name);
        }

        public override void Sync(ICollection<string> names)
        {
            // Intentionally left blank: Azure blob output is already 'stable storage'.
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                IsOpen = false;
            }
        }

        #endregion

        #region Private Methods

        private IndexInput GetIndexInput(string name)
        {
            BlobClient blobClient = GetBlobClient(name);
            BlobRequestConditions requestConditions = null;

            if (cachedInputs.Count > 0 && cachedInputs.TryGetValue(name, out CachedInput cachedInput))
            {
                // Use an access condition with 'If-None-Match' using the ETag of the currently cached version.
                requestConditions = new BlobRequestConditions()
                {
                    IfNoneMatch = cachedInput.ETag,
                };
            }
            else
            {
                cachedInput = null;
            }

            try
            {
                Response<BlobDownloadInfo> response = blobClient.Download(conditions: requestConditions);
                if (ShouldCache(name))
                {
                    if (null != cachedInput && response.GetRawResponse().Status == 304)
                    {
                        // The ETag hasn't changed because the content hasn't changed, so we can return the cached data.
                        return new RAMInputStream(name, cachedInput.File);
                    }

                    // Since we cache the downloaded content in memory, we need to dispose of the download info
                    // here and don't need to transfer ownership to another owner.
                    using (BlobDownloadInfo blobDownloadInfo = response.Value)
                    {
                        // Download the file into the cache, and serve it from there.
                        cachedInput = CachedInput.Create(name, blobDownloadInfo);
                        cachedInputs[name] = cachedInput;

                        return new RAMInputStream(name, cachedInput.File);
                    }
                }

                // This also transfers ownership of the BlobDownloadInfo from the response to the AzureBlobIndexInput.
                return new AzureBlobIndexInput(blobClient, response.Value);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                throw new FileNotFoundException($"The blob '{blobClient.Name}' does not exist.", name, ex);
            }
        }

        private bool ShouldCache(string name)
        {
            if (Options.CacheSegmentsGen)
            {
                return StringComparer.Ordinal.Equals(name, IndexFileNames.SEGMENTS_GEN);
            }

            return false;
        }

        #endregion
    }
}
