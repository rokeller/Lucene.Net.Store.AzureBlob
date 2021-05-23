using System;
using System.Threading;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Moq;
using Xunit;

namespace Lucene.Net.Store.AzureBlob.Test
{
    [Collection("AppInsights")]
    public sealed class AzureBlobLockTests : TestBase, IDisposable
    {
        private readonly CloudBlobContainer blobContainer;
        private AzureBlobLock theLock;

        public AzureBlobLockTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azurebloblock-test-" + Utils.GenerateRandomInt(1000));
            blobContainer.CreateIfNotExists();
        }

        public override void Dispose()
        {
            using (theLock) { }
            blobContainer.DeleteIfExists();
            base.Dispose();
        }

        [Fact]
        public void IsLockedWorks()
        {
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference("IsLockedWorks");
            theLock = new AzureBlobLock(blob);

            Assert.False(theLock.IsLocked());
            string leaseId = blob.AcquireLease(TimeSpan.FromSeconds(15) /* minimum allowed */, null);
            Assert.True(theLock.IsLocked());
            blob.ReleaseLease(AccessCondition.GenerateLeaseCondition(leaseId));
            Assert.False(theLock.IsLocked());
        }

        [Fact]
        public void ObtainWorks()
        {
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference("ObtainWorks");
            theLock = new AzureBlobLock(blob);

            Assert.True(theLock.Obtain());
            blob.FetchAttributes();
            Assert.Equal(LeaseStatus.Locked, blob.Properties.LeaseStatus);
            Assert.Equal(LeaseState.Leased, blob.Properties.LeaseState);
            Assert.False(theLock.Obtain());
        }

        [Fact]
        public void ObtainWorksInRaceCondition()
        {
            Mock<CloudBlockBlob> mockBlob = new Mock<CloudBlockBlob>(MockBehavior.Strict, new Uri("http://localhost:10000/devstorageaccount/ObtainWorksInRaceCondition"));
            theLock = new AzureBlobLock(mockBlob.Object);

            mockBlob.Setup(b => b.UploadFromByteArray(It.Is<byte[]>(buf => buf.Length == 0), 0, 0, null, null, null));
            mockBlob.Setup(b => b.AcquireLease(TimeSpan.FromMinutes(1), null, null, null, null))
                .Throws(new StorageException(new RequestResult() { HttpStatusCode = 409, }, "Injected error", null));
            mockBlob.Setup(b => b.Delete(DeleteSnapshotsOption.None, It.Is<AccessCondition>(c => c.LeaseId == null), null, null));

            Assert.False(theLock.Obtain());

            mockBlob.Verify(b => b.UploadFromByteArray(It.Is<byte[]>(buf => buf.Length == 0), 0, 0, null, null, null), Times.Once());
            mockBlob.Verify(b => b.AcquireLease(TimeSpan.FromMinutes(1), null, null, null, null), Times.Once());
        }

        [Fact]
        [Trait("Classification", "LongRunning")]
        public void LeaseRenewalWorks()
        {
            Mock<CloudBlockBlob> mockBlob = new Mock<CloudBlockBlob>(MockBehavior.Strict, new Uri("http://localhost:10000/devstorageaccount/LeaseRenewalWorks"));
            theLock = new AzureBlobLock(mockBlob.Object);

            mockBlob.Setup(b => b.UploadFromByteArray(It.Is<byte[]>(buf => buf.Length == 0), 0, 0, null, null, null));
            mockBlob.Setup(b => b.AcquireLease(TimeSpan.FromMinutes(1), null, null, null, null))
                .Returns("the-lease-id");
            mockBlob.Setup(b => b.RenewLease(It.Is<AccessCondition>(ac => ac.LeaseId == "the-lease-id"), null, null));
            mockBlob.Setup(b => b.Delete(DeleteSnapshotsOption.None, It.Is<AccessCondition>(c => c.LeaseId == "the-lease-id"), null, null));

            Assert.True(theLock.Obtain());
            Thread.Sleep(TimeSpan.FromSeconds(55.1));

            mockBlob.Verify(b => b.UploadFromByteArray(It.Is<byte[]>(buf => buf.Length == 0), 0, 0, null, null, null), Times.Once());
            mockBlob.Verify(b => b.AcquireLease(TimeSpan.FromMinutes(1), null, null, null, null), Times.Once());
            mockBlob.Verify(b => b.RenewLease(It.Is<AccessCondition>(ac => ac.LeaseId == "the-lease-id"), null, null), Times.Once());
        }
    }
}