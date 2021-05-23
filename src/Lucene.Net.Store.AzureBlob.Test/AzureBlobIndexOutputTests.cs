using System;
using Microsoft.Azure.Storage.Blob;
using Xunit;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public class AzureBlobIndexOutputTests : TestBase, IDisposable
    {
        private readonly CloudBlobContainer blobContainer;
        private AzureBlobIndexOutput output;

        public AzureBlobIndexOutputTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azureblobindexoutput-test-" + Utils.GenerateRandomInt(1000));
            blobContainer.CreateIfNotExists();
        }

        public override void Dispose()
        {
            using (output) { }
            blobContainer.DeleteIfExists();
            base.Dispose();
        }

        [Fact]
        public void LengthReportsCurrentLen()
        {
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference("LengthReportsCurrentLen");
            AzureBlobIndexOutput output;

            using (output = new AzureBlobIndexOutput(blob))
            {
                output.WriteByte(1);
            }
            Assert.Equal(1, output.Length);

            using (output = new AzureBlobIndexOutput(blob))
            {
                output.WriteBytes(Utils.GenerateRandomBuffer(1234), 0, 1234);
            }
            Assert.Equal(1234, output.Length);
        }

        [Fact]
        public void SeekThrowsNotSupported()
        {
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference("LengthReportsCurrentLen");
            output = new AzureBlobIndexOutput(blob);

            output.WriteString("SeekThrowsNotSupported");

#pragma warning disable 618
            Assert.Throws<NotSupportedException>(() => output.Seek(0));
#pragma warning restore 618
        }
    }
}