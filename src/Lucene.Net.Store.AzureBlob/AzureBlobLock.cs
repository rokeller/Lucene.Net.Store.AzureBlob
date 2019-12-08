using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    internal sealed class AzureBlobLock : Lock
    {
        private static readonly TimeSpan LeaseTime = TimeSpan.FromSeconds(60);

        private readonly CloudBlockBlob lockBlob;
        private string leaseId;

        private CancellationTokenSource renewalCancellationSource;

        public AzureBlobLock(CloudBlockBlob lockBlob)
        {
            this.lockBlob = lockBlob;
        }

        #region Lock Implementation

        public override bool IsLocked()
        {
            try
            {
                // If we can update the blob with a new block and without a lease ID, it isn't locked.
                // See https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-lease-operations-on-blobs-by-lease-state
                lockBlob.UploadFromByteArray(new byte[0], 0, 0);
                return false;
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 412) // Pre-condition failed
            {
                FailureReason = ex;
                return true;
            }
        }

        public override bool Obtain()
        {
            if (IsLocked())
            {
                return false;
            }

            try
            {
                leaseId = lockBlob.AcquireLease(LeaseTime, null);

                if (null == renewalCancellationSource)
                {
                    renewalCancellationSource = new CancellationTokenSource();
                    ScheduleRenewLease();
                }

                return true;
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 409) // Conflict -- Somebody else has a lease
            {
                FailureReason = ex;

                return false;
            }
        }

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
                    lockBlob.Delete(DeleteSnapshotsOption.None, AccessCondition.GenerateLeaseCondition(leaseId));
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
                lockBlob.RenewLease(AccessCondition.GenerateLeaseCondition(leaseId));

                ScheduleRenewLease();
            }
        }

        #endregion
    }
}