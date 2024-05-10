using System;
using System.Collections.Generic;
using Azure.Storage.Blobs;

namespace Lucene.Net.Store
{
    /// <summary>
    /// Implements the <see cref="AzureBlobLockFactory"/> using Azure blobs for
    /// locking.
    /// </summary>
    public class AzureBlobLockFactory : LockFactory
    {
        private readonly BlobContainerClient blobContainerClient;
        private readonly IBlobLeaseClientFactory blobLeaseClientFactory;
        private readonly object locksSyncRoot = new object();
        private readonly Dictionary<string, AzureBlobLock> locks = new Dictionary<string, AzureBlobLock>(StringComparer.Ordinal);

        /// <summary>
        /// Initializes a new instance of <see cref="AzureBlobLockFactory"/>.
        /// </summary>
        /// <param name="blobContainerClient">
        /// The <see cref="BlobContainerClient"/> to use for locking using blobs.
        /// </param>
        public AzureBlobLockFactory(BlobContainerClient blobContainerClient)
            : this(blobContainerClient, BlobLeaseClientFactory.Default)
        { }

        /// <summary>
        /// Initializes a new instance of <see cref="AzureBlobLockFactory"/>.
        /// </summary>
        /// <param name="blobContainerClient">
        /// The <see cref="BlobContainerClient"/> to use for locking using blobs.
        /// </param>
        /// <param name="blobLeaseClientFactory">
        /// The <see cref="IBlobLeaseClientFactory"/> to create blob lease clients.
        /// </param>
        private AzureBlobLockFactory(BlobContainerClient blobContainerClient, IBlobLeaseClientFactory blobLeaseClientFactory)
        {
            this.blobContainerClient = blobContainerClient;
            this.blobLeaseClientFactory = blobLeaseClientFactory;
        }

        /// <inheritdoc/>
        public override void ClearLock(string lockName)
        {
            string canonicalName = GetLockCanonicalName(lockName);

            lock (locksSyncRoot)
            {
                if (locks.TryGetValue(canonicalName, out AzureBlobLock l))
                {
                    locks.Remove(canonicalName);
                    l.Dispose();
                }
            }
        }

        /// <inheritdoc/>
        public override Lock MakeLock(string lockName)
        {
            string canonicalName = GetLockCanonicalName(lockName);

            lock (locksSyncRoot)
            {
                if (!locks.TryGetValue(canonicalName, out AzureBlobLock l))
                {
                    l = new AzureBlobLock(GetLockBlob(lockName), blobLeaseClientFactory);
                    locks.Add(canonicalName, l);
                }

                return l;
            }
        }

        private string GetLockCanonicalName(string lockName)
        {
            return LockPrefix + lockName;
        }

        private BlobClient GetLockBlob(string lockName)
        {
            return blobContainerClient.GetBlobClient(LockPrefix + lockName);
        }
    }
}
