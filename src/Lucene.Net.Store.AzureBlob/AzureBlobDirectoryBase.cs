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

        protected void DownloadBlobToFile(string sourceName, string targetPath)
        {
            CloudBlob blob = GetBlob(sourceName);

            try
            {
                blob.DownloadToFile(targetPath, FileMode.Create);
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 404)
            {
                throw new FileNotFoundException($"The blob '{blob.Name}' does not exist.", sourceName, ex);
            }
        }

        protected void UploadFileToBlob(string targetName, string sourcePath, AccessCondition accessCondition = null)
        {
            CloudBlockBlob blob = GetBlob(targetName);
            blob.UploadFromFile(sourcePath, accessCondition, null, null);
            lastKnownBlobs.TryAdd(targetName, blob);
        }

        protected void DeleteBlob(string name)
        {
            CloudBlockBlob blob = GetBlob(name);
            blob.DeleteIfExists();
            lastKnownBlobs.TryRemove(name, out _);
        }

        protected bool BlobExists(string name)
        {
            if (lastKnownBlobs.ContainsKey(name))
            {
                return true;
            }

            CloudBlockBlob blob = GetBlob(name);

            return blob.Exists();
        }

        protected long GetBlobLength(string name)
        {
            if (!lastKnownBlobs.TryGetValue(name, out CloudBlob blob))
            {
                blob = GetBlob(name);
                try
                {
                    blob.FetchAttributes();
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