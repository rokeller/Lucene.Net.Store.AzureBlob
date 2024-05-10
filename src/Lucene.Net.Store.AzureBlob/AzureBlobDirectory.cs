using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Lucene.Net.Index;

namespace Lucene.Net.Store
{
    /// <summary>
    /// Implements a <see cref="Directory"/> using Azure blobs to persist
    /// the files of the directory.
    /// </summary>
    public class AzureBlobDirectory : AzureBlobDirectoryBase
    {
        private readonly ConcurrentDictionary<string, CachedInput> cachedInputs = new ConcurrentDictionary<string, CachedInput>(StringComparer.Ordinal);

        /// <summary>
        /// Initializes a new instance of <see cref="AzureBlobDirectory"/>.
        /// </summary>
        /// <param name="blobContainerClient">
        /// The <see cref="BlobContainerClient"/> to use to manage blobs.
        /// </param>
        /// <param name="blobPrefix">
        /// The prefix to use for all blobs.
        /// </param>
        public AzureBlobDirectory(BlobContainerClient blobContainerClient, string blobPrefix) : this(blobContainerClient, blobPrefix, null)
        { }

        /// <summary>
        /// Initializes a new instance of <see cref="AzureBlobDirectory"/>.
        /// </summary>
        /// <param name="blobContainerClient">
        /// The <see cref="BlobContainerClient"/> to use to manage blobs.
        /// </param>
        /// <param name="blobPrefix">
        /// The prefix to use for all blobs.
        /// </param>
        /// <param name="options">
        /// The <see cref="AzureBlobDirectoryOptions"/> to use.
        /// </param>
        public AzureBlobDirectory(BlobContainerClient blobContainerClient, string blobPrefix, AzureBlobDirectoryOptions options)
        : base(blobContainerClient, blobPrefix, options)
        { }

        #region Directory Implementation

        /// <inheritdoc/>
        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            EnsureOpen();

            return new AzureBlobIndexOutput(GetBlockBlobClient(name));
        }

        /// <inheritdoc/>
        public override void DeleteFile(string name)
        {
            EnsureOpen();

            DeleteBlob(name);
        }

        /// <inheritdoc/>
        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name)
        {
            EnsureOpen();

            return BlobExists(name);
        }

        /// <inheritdoc/>
        public override long FileLength(string name)
        {
            EnsureOpen();

            return GetBlobLength(name);
        }

        /// <inheritdoc/>
        public override string[] ListAll()
        {
            EnsureOpen();

            string[] names = ListBlobs().Select(ExtractBlobName).ToArray();

            return names;
        }

        /// <inheritdoc/>
        public override IndexInput OpenInput(string name, IOContext context)
        {
            EnsureOpen();

            return GetIndexInput(name);
        }

        /// <inheritdoc/>
        public override void Sync(ICollection<string> names)
        {
            // Intentionally left blank: Azure blob output is already 'stable storage'.
        }

        /// <inheritdoc/>
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
            bool shouldCache = ShouldCache(name);

            if (shouldCache && cachedInputs.TryGetValue(name, out CachedInput cachedInput))
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
                if (shouldCache)
                {
                    // We should cache the file; make sure we already use the latest version or download and cache it.
                    Response<BlobDownloadInfo> response = blobClient.Download(conditions: requestConditions);
                    using (Response rawResponse = response.GetRawResponse())
                    {
                        if (null == cachedInput || rawResponse.Status == 200)
                        {
                            // The response status code can be 304 only when we already have the same version of the
                            // blob in our cache. Otherwise, if the input is not yet cached, or if the content has
                            // changed, we must have received the blob's content with a status code of 200.
                            Debug.Assert(rawResponse.Status == 200, "We must have received the blob content.");

                            using (BlobDownloadInfo blobDownloadInfo = response.Value)
                            {
                                // Create a new cached IndexInput with the blob's content.
                                cachedInput = CachedInput.Create(name, blobDownloadInfo);
                                cachedInputs[name] = cachedInput;
                            }
                        }

                        Debug.Assert(cachedInputs.ContainsKey(name), "The file must exist in the cache.");

                        // Serve the file from the cache.
                        return new RAMInputStream(name, cachedInput.File);
                    }
                }

                Stream stream = blobClient.OpenRead();
                // Pass ownership of the stream to the new AzureBlobIndexInput object, which must dispose the stream.
                return new AzureBlobIndexInput(blobClient, stream.Length, stream);
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
