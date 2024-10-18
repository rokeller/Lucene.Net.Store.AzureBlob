using System;
using Azure.Storage.Blobs;
using Xunit;

namespace Lucene.Net.Store.AzureBlob.Test
{
    [Collection("AppInsights")]
    public sealed class AzureBlobLockFactoryTests : TestBase, IDisposable
    {
        private readonly BlobContainerClient blobContainerClient;
        private readonly string lockName = Utils.GenerateRandomString(10);
        private AzureBlobLockFactory factory;

        public AzureBlobLockFactoryTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            blobContainerClient = Utils.GetBlobContainerClient("azurebloblockfactory-test-" + Utils.GenerateRandomInt(1000));
            blobContainerClient.CreateIfNotExists();
        }

        public override void Dispose()
        {
            factory.ClearLock(lockName);
            blobContainerClient.DeleteIfExists();
            base.Dispose();
        }

        [Fact]
        public void MakeAndClearWorkInTandem()
        {
            factory = new AzureBlobLockFactory(blobContainerClient);

            factory.MakeLock(lockName);
            Assert.Empty(blobContainerClient.GetBlobs());
            factory.ClearLock(lockName);
        }
    }
}