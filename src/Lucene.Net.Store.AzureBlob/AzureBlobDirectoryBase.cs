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
    /// <summary>
    /// Abstrace base class for <see cref="Directory"/> implementations using
    /// Azure blobs for storage.
    /// </summary>
    public abstract class AzureBlobDirectoryBase : BaseDirectory
    {
        private readonly BlobContainerClient blobContainerClient;
        private readonly string blobPrefix;

        private readonly ConcurrentDictionary<string, long> lastKnownBlobSizes =
            new ConcurrentDictionary<string, long>(StringComparer.Ordinal);

        /// <summary>
        /// Initializes a new instance of <see cref="AzureBlobDirectoryBase"/>.
        /// </summary>
        /// <param name="blobContainerClient">
        /// The <see cref="BlobContainerClient"/> to use for managing blobs.
        /// /// </param>
        /// <param name="blobPrefix">
        /// The prefix to use for blogs tracking files in the directory.
        /// </param>
        /// <param name="options">
        /// The <see cref="AzureBlobDirectoryOptions"/> to use for options.
        /// </param>
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

        /// <inheritdoc/>
        public override string GetLockID()
        {
            return blobPrefix;
        }

        #endregion

        #region Protected Interface

        /// <summary>
        /// Gets the <see cref="AzureBlobDirectoryOptions"/> that are used.
        /// </summary>
        protected AzureBlobDirectoryOptions Options { get; }

        /// <summary>
        /// Checks if a file with the given name exists in the set of blobs
        /// recently sync'ed from Azure blob storage.
        /// </summary>
        /// <param name="name">
        /// The name of the blob to check.
        /// </param>
        /// <returns>
        /// <c>true</c> if the blob is already known, <c>false</c> otherwise.
        /// </returns>
        protected bool IsInLastKnownBlobs(string name)
        {
            return lastKnownBlobSizes.ContainsKey(name);
        }

        /// <summary>
        /// Downloads a blob with the given <paramref name="sourceName"/> to the
        /// specified <paramref name="targetPath"/>.
        /// </summary>
        /// <param name="sourceName">
        /// The name of the blob to download.
        /// </param>
        /// <param name="targetPath">
        /// The path to write the downloaded blob contents to.
        /// </param>
        /// <exception cref="FileNotFoundException">
        /// Thrown if the blob cannot be found.
        /// </exception>
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

        /// <summary>
        /// Uploads the file from <paramref name="sourcePath"/> to a blob named
        /// <paramref name="targetName"/> using the given <paramref name="uploadOptions"/>.
        /// </summary>
        /// <param name="targetName">
        /// The name of the blob to upload to.
        /// </param>
        /// <param name="sourcePath">
        /// The local path of the file to upload.
        /// </param>
        /// <param name="uploadOptions">
        /// The <see cref="BlobUploadOptions"/> to use for the upload.
        /// </param>
        protected void UploadFileToBlob(string targetName, string sourcePath, BlobUploadOptions uploadOptions = null)
        {
            BlobClient blobClient = GetBlobClient(targetName);
            blobClient.Upload(sourcePath, uploadOptions ?? new BlobUploadOptions());

            // This is ugly yet the best option available to determine the size of the blob we've just uploaded -- the
            // Blob REST APIs don't give us the size of the uploaded content.
            FileInfo file = new FileInfo(sourcePath);
            lastKnownBlobSizes.TryAdd(targetName, file.Length);
        }

        /// <summary>
        /// Deletes the blob named <paramref name="name"/> from Azure blob storage.
        /// </summary>
        /// <param name="name">
        /// The name of the blob to delete.
        /// </param>
        protected void DeleteBlob(string name)
        {
            BlobClient blobClient = GetBlobClient(name);
            blobClient.DeleteIfExists();
            lastKnownBlobSizes.TryRemove(name, out _);
        }

        /// <summary>
        /// Checks if a blob with the given name exists.
        /// </summary>
        /// <param name="name">
        /// The name of the blob to check.
        /// </param>
        /// <returns>
        /// <c>true</c> if the blob exists, <c>false</c> otherwise.
        /// </returns>
        protected bool BlobExists(string name)
        {
            if (lastKnownBlobSizes.ContainsKey(name))
            {
                return true;
            }

            BlobClient blobClient = GetBlobClient(name);

            return blobClient.Exists();
        }

        /// <summary>
        /// Gets the length (size) of the given name.
        /// </summary>
        /// <param name="name">
        /// The name of the blob to get the length for.
        /// </param>
        /// <returns>
        /// The length (in bytes) of the blob named <paramref name="name"/>.
        /// </returns>
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

        /// <summary>
        /// Lists the current blobs.
        /// </summary>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> of <see cref="BlobItem"/> representing
        /// all known blobs with the current prefix.
        /// </returns>
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

        /// <summary>
        /// Extracts the name of the blob from the given <paramref name="blob"/>.
        /// </summary>
        /// <param name="blob">
        /// A <see cref="BlobItem"/> from which to get the name.
        /// </param>
        /// <returns>
        /// The name of the blob without its path.
        /// </returns>
        protected string ExtractBlobName(BlobItem blob)
        {
            return Path.GetFileName(blob.Name);
        }

        /// <summary>
        /// Gets a <see cref="BlobClient"/> for the blob with the given
        /// <paramref name="name"/>.
        /// </summary>
        /// <param name="name">
        /// The name of the blob to get a <see cref="BlobClient"/> for.
        /// </param>
        /// <returns>
        /// A <see cref="BlobClient"/> that can be used to manage the blob
        /// given by <paramref name="name"/>.
        /// </returns>
        protected BlobClient GetBlobClient(string name)
        {
            return blobContainerClient.GetBlobClient(blobPrefix + name);
        }

        /// <summary>
        /// Gets a <see cref="BlockBlobClient"/> for the blob with the given
        /// <paramref name="name"/>.
        /// </summary>
        /// <param name="name">
        /// The name of the blob to get a <see cref="BlockBlobClient"/> for.
        /// </param>
        /// <returns>
        /// A <see cref="BlockBlobClient"/> that can be used to manage the blob
        /// given by <paramref name="name"/>.
        /// </returns>
        protected BlockBlobClient GetBlockBlobClient(string name)
        {
            return blobContainerClient.GetBlockBlobClient(blobPrefix + name);
        }

        #endregion
    }
}
