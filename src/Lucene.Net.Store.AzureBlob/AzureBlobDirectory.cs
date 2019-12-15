using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Lucene.Net.Index;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    public class AzureBlobDirectory : BaseDirectory
    {
        private readonly CloudBlobContainer blobContainer;
        private readonly string blobPrefix;
        private readonly AzureBlobDirectoryOptions options;
        private readonly ConcurrentDictionary<string, CachedInput> cachedInputs = new ConcurrentDictionary<string, CachedInput>(StringComparer.Ordinal);

        public AzureBlobDirectory(CloudBlobContainer blobContainer, string blobPrefix) : this(blobContainer, blobPrefix, null)
        {
        }

        public AzureBlobDirectory(CloudBlobContainer blobContainer, string blobPrefix, AzureBlobDirectoryOptions options)
        {
            this.blobContainer = blobContainer;

            if (null == blobPrefix)
            {
                blobPrefix = String.Empty;
            }
            else if (!blobPrefix.EndsWith("/"))
            {
                blobPrefix += "/";
            }

            this.blobPrefix = blobPrefix;
            this.options = options ?? new AzureBlobDirectoryOptions();

            SetLockFactory(new AzureBlobLockFactory(blobContainer));
        }

        #region Directory Implementation

        public override string GetLockID()
        {
            return $"{blobPrefix}AzureBlobDirectory[{blobContainer.Uri.Host}][{blobContainer.Name}]";
        }

        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            EnsureOpen();

            return new AzureBlobIndexOutput(GetBlob(name));
        }

        public override void DeleteFile(string name)
        {
            EnsureOpen();

            CloudBlockBlob blob = GetBlob(name);

            blob.DeleteIfExists();
        }

        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name)
        {
            EnsureOpen();

            CloudBlockBlob blob = GetBlob(name);

            return blob.Exists();
        }

        public override long FileLength(string name)
        {
            EnsureOpen();

            CloudBlockBlob blob = GetBlobWithMetaOrThrowIfNotFound(name);

            return blob.Properties.Length;
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

            Stream stream;
            CachedInput cachedInput;
            CloudBlockBlob blob = GetBlobWithStreamOrThrowIfNotFound(name, out stream, out cachedInput);

            if (null == blob && null != cachedInput)
            {
                // We had a hit in the cache, so let's use the cached file.
                return new RAMInputStream(name, cachedInput.File);
            }

            return new AzureBlobIndexInput(blob, stream);
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

        private CloudBlockBlob GetBlob(string name)
        {
            return blobContainer.GetBlockBlobReference(blobPrefix + name);
        }

        private CloudBlockBlob GetBlobWithMetaOrThrowIfNotFound(string name)
        {
            CloudBlockBlob blob = GetBlob(name);

            try
            {
                blob.FetchAttributes();
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 404)
            {
                throw new FileNotFoundException($"The blob '{blob.Name}' does not exist.", name, ex);
            }

            return blob;
        }

        private CloudBlockBlob GetBlobWithStreamOrThrowIfNotFound(string name, out Stream stream, out CachedInput cachedInput)
        {
            CloudBlockBlob blob = GetBlob(name);
            AccessCondition accessCondition = null;

            if (cachedInputs.Count > 0 && cachedInputs.TryGetValue(name, out cachedInput))
            {
                // Use an access condition with 'If-None-Match' using the ETag of the currently cached version.
                accessCondition = AccessCondition.GenerateIfNoneMatchCondition(cachedInput.ETag);
            }
            else
            {
                cachedInput = null;
            }

            try
            {
                stream = blob.OpenRead(accessCondition);
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 304) // Condition not met: The ETag hasn't changed, return the cached data.
            {
                // No need to download the data again, since it hasn't changed.
                Debug.Assert(null != cachedInput, "The cached input must not be null.");
                stream = null;

                return null;
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 404)
            {
                throw new FileNotFoundException($"The blob '{blob.Name}' does not exist.", name, ex);
            }

            if (ShouldCache(name))
            {
                // Download the file into the cache, and serve it from there.
                cachedInput = CachedInput.Create(name, blob, stream);
                cachedInputs[name] = cachedInput;
                stream = null;

                return null;
            }

            return blob;
        }

        private bool ShouldCache(string name)
        {
            if (options.CacheSegmentsGen)
            {
                return StringComparer.Ordinal.Equals(name, IndexFileNames.SEGMENTS_GEN);
            }

            return false;
        }

        private IEnumerable<CloudBlob> ListBlobs()
        {
            return blobContainer.ListBlobs(blobPrefix, false, BlobListingDetails.None).OfType<CloudBlob>();
        }

        private string ExtractBlobName(CloudBlob blob)
        {
            return Path.GetFileName(blob.Name);
        }

        #endregion
    }
}
