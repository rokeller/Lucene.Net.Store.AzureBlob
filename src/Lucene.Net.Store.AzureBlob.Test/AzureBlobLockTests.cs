using System.IO;
using System;
using System.Threading;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Moq;
using Xunit;

namespace Lucene.Net.Store.AzureBlob.Test
{
    [Collection("AppInsights")]
    public sealed class AzureBlobLockTests : TestBase, IDisposable
    {
        private readonly BlobContainerClient blobContainerClient;
        private AzureBlobLock theLock;

        public AzureBlobLockTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            blobContainerClient = Utils.GetBlobContainerClient("azurebloblock-test-" + Utils.GenerateRandomInt(1000));
            blobContainerClient.CreateIfNotExists();
        }

        public override void Dispose()
        {
            using (theLock) { }
            blobContainerClient.DeleteIfExists();
            base.Dispose();
        }

        [Fact]
        public void IsLockedWorks()
        {
            BlobClient blobClient = blobContainerClient.GetBlobClient("IsLockedWorks");
            BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient();
            theLock = new AzureBlobLock(blobClient, BlobLeaseClientFactory.Default);

            Assert.False(theLock.IsLocked());
            blobLeaseClient.Acquire(TimeSpan.FromSeconds(15) /* minimum allowed */);
            Assert.True(theLock.IsLocked());
            blobLeaseClient.Renew();
            Assert.True(theLock.IsLocked());
            blobLeaseClient.Release();
            Assert.False(theLock.IsLocked());
        }

        [Fact]
        public void ObtainWorks()
        {
            BlobClient blobClient = blobContainerClient.GetBlobClient("ObtainWorks");
            BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient();
            theLock = new AzureBlobLock(blobClient, BlobLeaseClientFactory.Default);

            Assert.True(theLock.Obtain());
            Response<BlobProperties> response = blobClient.GetProperties();
            Assert.Equal(LeaseStatus.Locked, response.Value.LeaseStatus);
            Assert.Equal(LeaseState.Leased, response.Value.LeaseState);
            Assert.False(theLock.Obtain());
        }

        [Fact]
        public void ObtainWorksInRaceCondition()
        {
            Mock<BlobClient> mockBlobClient = new Mock<BlobClient>(MockBehavior.Strict,
                new Uri("http://localhost:10000/devstorageaccount/ObtainWorksInRaceCondition"), (BlobClientOptions)null);
            Mock<BlobLeaseClient> mockBlobLeaseClient = new Mock<BlobLeaseClient>(MockBehavior.Strict,
                mockBlobClient.Object, (string)null);
            Mock<IBlobLeaseClientFactory> mockLeaseClientFactory = new Mock<IBlobLeaseClientFactory>(MockBehavior.Strict);

            mockLeaseClientFactory
                .Setup(f => f.GetBlobLeaseClient(mockBlobClient.Object))
                .Returns(mockBlobLeaseClient.Object);
            mockBlobClient
                .Setup(c => c.Upload(Stream.Null, true, default))
                .Returns(new TestResponse<BlobContentInfo>(BlobsModelFactory.BlobContentInfo(
                    new ETag("abcd"),
                    DateTimeOffset.UtcNow,
                    null, null, null, null, 1)));
            mockBlobLeaseClient
                .Setup(c => c.Acquire(TimeSpan.FromMinutes(1), null, default(CancellationToken)))
                .Throws(new RequestFailedException(409, "Injected error"));

            theLock = new AzureBlobLock(mockBlobClient.Object, mockLeaseClientFactory.Object);
            Assert.False(theLock.Obtain());

            mockLeaseClientFactory.Verify(f => f.GetBlobLeaseClient(It.IsAny<BlobBaseClient>()), Times.Once());
            mockBlobClient.Verify(c => c.Upload(It.IsAny<Stream>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once());
            mockBlobLeaseClient.Verify(c => c.Acquire(It.IsAny<TimeSpan>(), It.IsAny<RequestConditions>(), It.IsAny<CancellationToken>()), Times.Once());
        }

        [Fact]
        [Trait("Classification", "LongRunning")]
        public void LeaseRenewalWorks()
        {
            Mock<BlobClient> mockBlobClient = new Mock<BlobClient>(MockBehavior.Strict,
                new Uri("http://localhost:10000/devstorageaccount/LeaseRenewalWorks"), (BlobClientOptions)null);
            Mock<BlobLeaseClient> mockBlobLeaseClient = new Mock<BlobLeaseClient>(MockBehavior.Strict,
                mockBlobClient.Object, (string)null);
            Mock<IBlobLeaseClientFactory> mockLeaseClientFactory = new Mock<IBlobLeaseClientFactory>(MockBehavior.Strict);

            mockLeaseClientFactory
                .Setup(f => f.GetBlobLeaseClient(mockBlobClient.Object))
                .Returns(mockBlobLeaseClient.Object);
            mockBlobClient
                .Setup(c => c.Upload(Stream.Null, true, default))
                .Returns(new TestResponse<BlobContentInfo>(BlobsModelFactory.BlobContentInfo(
                    new ETag("1234"),
                    DateTimeOffset.UtcNow,
                    null, null, null, null, 1)));
            mockBlobLeaseClient
                .Setup(c => c.Acquire(TimeSpan.FromMinutes(1), null, default(CancellationToken)))
                .Returns(new TestResponse<BlobLease>(BlobsModelFactory.BlobLease(
                    new ETag("1234"),
                    DateTimeOffset.UtcNow,
                    "the-lease-id"
                )));
            mockBlobLeaseClient
                .Setup(c => c.Renew(null, default))
                .Returns(new TestResponse<BlobLease>(BlobsModelFactory.BlobLease(
                    new ETag("1234"),
                    DateTimeOffset.UtcNow,
                    "the-lease-id"
                )));
            mockBlobClient
                .Setup(c => c.Delete(DeleteSnapshotsOption.None, It.Is<BlobRequestConditions>(rc => rc.LeaseId == "the-lease-id"), default))
                .Returns(new TestResponse());

            theLock = new AzureBlobLock(mockBlobClient.Object, mockLeaseClientFactory.Object);
            Assert.True(theLock.Obtain());
            Thread.Sleep(TimeSpan.FromSeconds(55.1));

            mockLeaseClientFactory.Verify(f => f.GetBlobLeaseClient(It.IsAny<BlobBaseClient>()), Times.Once());
            mockBlobClient.Verify(c => c.Upload(It.IsAny<Stream>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once());
            mockBlobLeaseClient.Verify(c => c.Acquire(It.IsAny<TimeSpan>(), It.IsAny<RequestConditions>(), It.IsAny<CancellationToken>()), Times.Once());
            mockBlobLeaseClient.Verify(c => c.Renew(null, default), Times.AtMostOnce());
        }
    }
}
