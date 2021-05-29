using System;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Xunit;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public class AzureBlobIndexOutputTests : TestBase, IDisposable
    {
        private readonly BlobContainerClient blobContainerClient;
        private AzureBlobIndexOutput output;

        public AzureBlobIndexOutputTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            blobContainerClient = Utils.GetBlobContainerClient("azureblobindexoutput-test-" + Utils.GenerateRandomInt(1000));
            blobContainerClient.CreateIfNotExists();
        }

        public override void Dispose()
        {
            using (output) { }
            blobContainerClient.DeleteIfExists();
            base.Dispose();
        }

        [Fact]
        public void LengthReportsCurrentLen()
        {
            BlockBlobClient blobClient = blobContainerClient.GetBlockBlobClient("LengthReportsCurrentLen");
            AzureBlobIndexOutput output;

            using (output = new AzureBlobIndexOutput(blobClient))
            {
                output.WriteByte(1);
            }
            Assert.Equal(1, output.Length);

            using (output = new AzureBlobIndexOutput(blobClient))
            {
                output.WriteBytes(Utils.GenerateRandomBuffer(1234), 0, 1234);
            }
            Assert.Equal(1234, output.Length);
        }

        [Fact]
        public void SeekThrowsNotSupported()
        {
            BlockBlobClient blobClient = blobContainerClient.GetBlockBlobClient("LengthReportsCurrentLen");
            output = new AzureBlobIndexOutput(blobClient);

            output.WriteString("SeekThrowsNotSupported");

#pragma warning disable 618
            Assert.Throws<NotSupportedException>(() => output.Seek(0));
#pragma warning restore 618
        }
    }
}