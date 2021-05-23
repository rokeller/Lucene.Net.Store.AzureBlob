using System;
using System.IO;
using Microsoft.Azure.Storage.Blob;
using Xunit;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public sealed class AzureBlobIndexInputTests : TestBase, IDisposable
    {
        private readonly CloudBlobContainer blobContainer;
        private AzureBlobIndexInput input;

        public AzureBlobIndexInputTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azureblobindexinput-test-" + Utils.GenerateRandomInt(1000));
            blobContainer.CreateIfNotExists();
        }

        public override void Dispose()
        {
            using (input) { }
            blobContainer.DeleteIfExists();
            base.Dispose();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(137)]
        [InlineData(16 * 1024 + 17)]
        public void ReadByteThrowsForEof(int len)
        {
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference("ReadByteThrowsForEof");

            blob.UploadFromByteArray(Utils.GenerateRandomBuffer(len), 0, len);

            Stream stream = blob.OpenRead();
            input = new AzureBlobIndexInput(blob, stream);

            if (len > 1)
            {
                byte[] preBuf = new byte[len - 1];
                input.ReadBytes(preBuf, 0, len - 1);
            }

            input.ReadByte();

            Assert.Throws<EndOfStreamException>(() => input.ReadByte());
        }
    }
}