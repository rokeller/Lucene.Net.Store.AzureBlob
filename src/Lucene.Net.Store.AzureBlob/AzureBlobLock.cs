using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Lucene.Net.Store
{
    internal sealed class AzureBlobLock : Lock
    {
        private static readonly TimeSpan LeaseTime = TimeSpan.FromSeconds(60);

        private readonly BlobClient lockBlobClient;
        private readonly BlobLeaseClient leaseClient;
        private string leaseId;

        private CancellationTokenSource renewalCancellationSource;

        public AzureBlobLock(BlobClient lockBlobClient, IBlobLeaseClientFactory blobLeaseClientFactory)
        {
            this.lockBlobClient = lockBlobClient;
            leaseClient = blobLeaseClientFactory.GetBlobLeaseClient(lockBlobClient);
        }

        #region Lock Implementation

        /// <inheritdoc/>
        public override bool IsLocked()
        {
            try
            {
                // If we can update the blob with empty data and _without_ a lease ID, it isn't locked.
                // See https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-lease-operations-on-blobs-by-lease-state
                lockBlobClient.Upload(Stream.Null, overwrite: true);
                return false;
            }
            catch (RequestFailedException ex) when (ex.Status == 412) // Pre-condition failed
            {
                FailureReason = ex;
                return true;
            }
        }

        /// <inheritdoc/>
        public override bool Obtain()
        {
            if (IsLocked())
            {
                return false;
            }

            try
            {
                Response<BlobLease> response = leaseClient.Acquire(LeaseTime);
                leaseId = response.Value.LeaseId;

                if (null == renewalCancellationSource)
                {
                    renewalCancellationSource = new CancellationTokenSource();
                    ScheduleRenewLease();
                }

                return true;
            }
            catch (RequestFailedException ex) when (ex.Status == 409) // Conflict -- Somebody else has a lease
            {
                FailureReason = ex;

                return false;
            }
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Cleanup the renewal canncellation source first. We wouldn't want the renewal to renew again while we're trying
                // to release the lease and delete the blob.
                if (null != renewalCancellationSource)
                {
                    renewalCancellationSource.Cancel();
                    renewalCancellationSource.Dispose();
                    renewalCancellationSource = null;
                }

                if (null != leaseId)
                {
                    // Delete the blob using the current lease.
                    BlobRequestConditions conditions = new BlobRequestConditions()
                    {
                        LeaseId = leaseId,
                    };
                    lockBlobClient.Delete(DeleteSnapshotsOption.None, conditions);
                    leaseId = null;
                }
            }
        }

        #endregion

        #region Private Methods

        private void ScheduleRenewLease()
        {
            Task
                .Delay(LeaseTime.Subtract(TimeSpan.FromSeconds(5)), renewalCancellationSource.Token)
                .ContinueWith(RenewLease, TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        private void RenewLease(Task task)
        {
            // Only renew if the renewal hasn't been cancelled yet.
            if (!renewalCancellationSource.IsCancellationRequested)
            {
                // See https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-lease-operations-on-blobs-by-lease-state
                leaseClient.Renew();

                ScheduleRenewLease();
            }
        }

        #endregion
    }
}
