using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    public abstract class AzureBlobDirectoryBase : BaseDirectory
    {
        private readonly CloudBlobContainer blobContainer;
        private readonly string blobPrefix;

        private readonly ConcurrentDictionary<string, CachedInput> cachedInputs =
            new ConcurrentDictionary<string, CachedInput>(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, CloudBlob> lastKnownBlobs =
            new ConcurrentDictionary<string, CloudBlob>(StringComparer.Ordinal);

        public AzureBlobDirectoryBase(CloudBlobContainer blobContainer, string blobPrefix, AzureBlobDirectoryOptions options)
        {
            this.blobContainer = blobContainer ?? throw new ArgumentNullException(nameof(blobContainer));

            if (null == blobPrefix)
            {
                blobPrefix = String.Empty;
            }
            else if (!blobPrefix.EndsWith("/"))
            {
                blobPrefix += "/";
            }

            this.blobPrefix = blobPrefix;
            Options = options ?? new AzureBlobDirectoryOptions();

            SetLockFactory(new AzureBlobLockFactory(blobContainer));
        }

        #region Directory Implementation

        public override string GetLockID()
        {
            return blobPrefix;
        }

        #endregion

        #region Protected Interface

        protected AzureBlobDirectoryOptions Options { get; }

        protected bool IsInLastKnownBlobs(string name)
        {
            return lastKnownBlobs.ContainsKey(name);
        }

        protected async Task DownloadFileAsync(string sourceName, string targetPath)
        {
            CloudBlob blob = GetBlob(sourceName);

            try
            {
                await blob.DownloadToFileAsync(targetPath, FileMode.Create);
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 404)
            {
                throw new FileNotFoundException($"The blob '{blob.Name}' does not exist.", sourceName, ex);
            }
        }

        protected async Task UploadFileAsync(string targetName, string sourcePath, AccessCondition accessCondition = null)
        {
            CloudBlockBlob blob = GetBlob(targetName);
            await blob.UploadFromFileAsync(sourcePath, accessCondition, null, null);
            lastKnownBlobs.TryAdd(targetName, blob);
        }

        protected async Task DeleteFileAsync(string name)
        {
            CloudBlockBlob blob = GetBlob(name);
            await blob.DeleteIfExistsAsync();
            lastKnownBlobs.TryRemove(name, out _);
        }

        protected Task<bool> FileExistsAsync(string name)
        {
            if (lastKnownBlobs.ContainsKey(name))
            {
                return Task.FromResult(true);
            }

            CloudBlockBlob blob = GetBlob(name);

            return blob.ExistsAsync();
        }

        protected async Task<long> GetFileLengthAsync(string name)
        {
            if (!lastKnownBlobs.TryGetValue(name, out CloudBlob blob))
            {
                blob = GetBlob(name);
                try
                {
                    await blob.FetchAttributesAsync();
                }
                catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 404)
                {
                    throw new FileNotFoundException($"The blob '{name}' does not exist.", name, ex);
                }

                lastKnownBlobs.TryAdd(name, blob);
            }

            return blob.Properties.Length;
        }

        protected IEnumerable<CloudBlob> ListBlobs()
        {
            IEnumerable<CloudBlob> blobs = blobContainer
                .ListBlobs(blobPrefix, useFlatBlobListing: false, BlobListingDetails.Metadata)
                .OfType<CloudBlob>();
            List<CloudBlob> blobList = new List<CloudBlob>();

            lastKnownBlobs.Clear();

            foreach (CloudBlob blob in blobs)
            {
                string name = ExtractBlobName(blob);
                lastKnownBlobs.TryAdd(name, blob);
                blobList.Add(blob);
            }

            return blobList;
        }

        protected string ExtractBlobName(CloudBlob blob)
        {
            return Path.GetFileName(blob.Name);
        }

        protected CloudBlockBlob GetBlob(string name)
        {
            return blobContainer.GetBlockBlobReference(blobPrefix + name);
        }

        #endregion
    }
}