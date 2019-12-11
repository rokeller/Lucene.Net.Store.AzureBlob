using Microsoft.Azure.Storage.Blob;
using Xunit;

namespace Lucene.Net.Store.AzureBlob.Test
{
    public sealed class AzureBlobLockFactoryTests
    {
        private readonly CloudBlobContainer blobContainer;
        private readonly string lockName = Utils.GenerateRandomString(10);
        private AzureBlobLockFactory factory;

        public AzureBlobLockFactoryTests()
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azurebloblockfactory-test");
            blobContainer.CreateIfNotExists();
        }

        public void Dispose()
        {
            factory.ClearLock(lockName);
            blobContainer.DeleteIfExists();
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