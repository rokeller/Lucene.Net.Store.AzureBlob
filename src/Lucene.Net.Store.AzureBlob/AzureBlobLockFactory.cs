using System;
using System.Collections.Generic;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    public class AzureBlobLockFactory : LockFactory
    {
        private readonly CloudBlobContainer blobContainer;
        private readonly object locksSyncRoot = new object();
        private readonly Dictionary<string, AzureBlobLock> locks = new Dictionary<string, AzureBlobLock>(StringComparer.Ordinal);

        public AzureBlobLockFactory(CloudBlobContainer blobContainer)
        {
            this.blobContainer = blobContainer;
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
                    locks.Add(canonicalName, l = new AzureBlobLock(GetLockBlob(lockName)));
                }

                return l;
            }
        }

        private string GetLockCanonicalName(string lockName)
        {
            return LockPrefix + lockName;
        }

        private CloudBlockBlob GetLockBlob(string lockName)
        {
            return blobContainer.GetBlockBlobReference(LockPrefix + lockName);
        }
    }
}