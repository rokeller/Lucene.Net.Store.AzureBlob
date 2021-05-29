using System;
using System.Collections.Generic;
using Azure.Storage.Blobs;

namespace Lucene.Net.Store
{
    public class AzureBlobLockFactory : LockFactory
    {
        private readonly BlobContainerClient blobContainerClient;
        private readonly IBlobLeaseClientFactory blobLeaseClientFactory;
        private readonly object locksSyncRoot = new object();
        private readonly Dictionary<string, AzureBlobLock> locks = new Dictionary<string, AzureBlobLock>(StringComparer.Ordinal);

        public AzureBlobLockFactory(BlobContainerClient blobContainerClient)
            : this(blobContainerClient, BlobLeaseClientFactory.Default)
        { }

        private AzureBlobLockFactory(BlobContainerClient blobContainerClient, IBlobLeaseClientFactory blobLeaseClientFactory)
        {
            this.blobContainerClient = blobContainerClient;
            this.blobLeaseClientFactory = blobLeaseClientFactory;
        }

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

        public override Lock MakeLock(string lockName)
        {
            string canonicalName = GetLockCanonicalName(lockName);

            lock (locksSyncRoot)
            {
                if (!locks.TryGetValue(canonicalName, out AzureBlobLock l))
                {
                    locks.Add(canonicalName, l = new AzureBlobLock(GetLockBlob(lockName), blobLeaseClientFactory));
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