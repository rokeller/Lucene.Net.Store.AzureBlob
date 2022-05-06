using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Lucene.Net.Store
{
    public abstract class AzureBlobDirectoryBase : BaseDirectory
    {
        private readonly BlobContainerClient blobContainerClient;
        private readonly string blobPrefix;

        private readonly ConcurrentDictionary<string, long> lastKnownBlobSizes =
            new ConcurrentDictionary<string, long>(StringComparer.Ordinal);

        protected AzureBlobDirectoryBase(BlobContainerClient blobContainerClient, string blobPrefix, AzureBlobDirectoryOptions options)
        {
            this.blobContainerClient = blobContainerClient ?? throw new ArgumentNullException(nameof(blobContainerClient));

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

            SetLockFactory(new AzureBlobLockFactory(blobContainerClient));
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
            return lastKnownBlobSizes.ContainsKey(name);
        }

        protected void DownloadBlobToFile(string sourceName, string targetPath)
        {
            BlobClient blobClient = GetBlobClient(sourceName);

            try
            {
                blobClient.DownloadTo(targetPath);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                throw new FileNotFoundException($"The blob '{blobClient.Name}' does not exist.", sourceName, ex);
            }
        }

        protected void UploadFileToBlob(string targetName, string sourcePath, BlobUploadOptions uploadOptions = null)
        {
            BlobClient blobClient = GetBlobClient(targetName);
            blobClient.Upload(sourcePath, uploadOptions ?? new BlobUploadOptions());

            // This is ugly yet the best option available to determine the size of the blob we've just uploaded -- the
            // Blob REST APIs don't give us the size of the uploaded content.
            FileInfo file = new FileInfo(sourcePath);
            lastKnownBlobSizes.TryAdd(targetName, file.Length);
        }

        protected void DeleteBlob(string name)
        {
            BlobClient blobClient = GetBlobClient(name);
            blobClient.DeleteIfExists();
            lastKnownBlobSizes.TryRemove(name, out _);
        }

        protected bool BlobExists(string name)
        {
            if (lastKnownBlobSizes.ContainsKey(name))
            {
                return true;
            }

            BlobClient blobClient = GetBlobClient(name);

            return blobClient.Exists();
        }

        protected long GetBlobLength(string name)
        {
            if (!lastKnownBlobSizes.TryGetValue(name, out long length))
            {
                BlobClient blobClient = GetBlobClient(name);
                try
                {
                    Response<BlobProperties> response = blobClient.GetProperties();
                    length = response.Value.ContentLength;
                    lastKnownBlobSizes.TryAdd(name, length);
                }
                catch (RequestFailedException ex) when (ex.Status == 404)
                {
                    throw new FileNotFoundException($"The blob '{name}' does not exist.", name, ex);
                }
            }

            return length;
        }

        protected IEnumerable<BlobItem> ListBlobs()
        {
            IEnumerable<BlobItem> blobs = blobContainerClient.GetBlobs(prefix: blobPrefix);
            List<BlobItem> blobList = new List<BlobItem>();

            lastKnownBlobSizes.Clear();

            foreach (BlobItem blob in blobs)
            {
                string name = ExtractBlobName(blob);
                lastKnownBlobSizes.TryAdd(name, blob.Properties.ContentLength.Value);
                blobList.Add(blob);
            }

            return blobList;
        }

        protected string ExtractBlobName(BlobItem blob)
        {
            return Path.GetFileName(blob.Name);
        }

        protected BlobClient GetBlobClient(string name)
        {
            return blobContainerClient.GetBlobClient(blobPrefix + name);
        }

        protected BlockBlobClient GetBlockBlobClient(string name)
        {
            return blobContainerClient.GetBlockBlobClient(blobPrefix + name);
        }

        #endregion
    }
}
