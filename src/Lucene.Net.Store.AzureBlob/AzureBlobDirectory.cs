using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    public class AzureBlobDirectory : BaseDirectory
    {
        private readonly CloudBlobContainer blobContainer;
        private readonly string blobPrefix;

        public AzureBlobDirectory(CloudBlobContainer blobContainer, string blobPrefix)
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
            CloudBlockBlob blob = GetBlobWithStreamOrThrowIfNotFound(name, out stream);

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

        private CloudBlockBlob GetBlobWithStreamOrThrowIfNotFound(string name, out Stream stream)
        {
            CloudBlockBlob blob = GetBlob(name);

            try
            {
                stream = blob.OpenRead();
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 404)
            {
                throw new FileNotFoundException($"The blob '{blob.Name}' does not exist.", name, ex);
            }

            return blob;
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
