using System;
using Microsoft.Azure.Storage.Blob;
using Xunit;

namespace Lucene.Net.Store.AzureBlob.Test
{
    [Collection("AppInsights")]
    public sealed class AzureBlobLockFactoryTests : TestBase, IDisposable
    {
        private readonly CloudBlobContainer blobContainer;
        private readonly string lockName = Utils.GenerateRandomString(10);
        private AzureBlobLockFactory factory;

        public AzureBlobLockFactoryTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azurebloblockfactory-test-" + Utils.GenerateRandomInt(1000));
            blobContainer.CreateIfNotExists();
        }

        public override void Dispose()
        {
            factory.ClearLock(lockName);
            blobContainer.DeleteIfExists();
            base.Dispose();
        }

        [Fact]
        public void MakeAndClearWorkInTandem()
        {
            factory = new AzureBlobLockFactory(blobContainer);

            factory.MakeLock(lockName);
            Assert.Empty(blobContainer.ListBlobs());
            factory.ClearLock(lockName);
        }
    }
}