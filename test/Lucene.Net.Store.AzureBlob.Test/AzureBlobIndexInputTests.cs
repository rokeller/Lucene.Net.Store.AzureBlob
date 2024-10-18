using System;
using System.IO;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Xunit;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public sealed class AzureBlobIndexInputTests : TestBase, IDisposable
    {
        private readonly BlobContainerClient blobContainerClient;
        private AzureBlobIndexInput input;

        public AzureBlobIndexInputTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            blobContainerClient = Utils.GetBlobContainerClient("azureblobindexinput-test-" + Utils.GenerateRandomInt(1000));
            blobContainerClient.CreateIfNotExists();
        }

        public override void Dispose()
        {
            using (input) { }
            blobContainerClient.DeleteIfExists();
            base.Dispose();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(137)]
        [InlineData(16 * 1024 + 17)]
        public void ReadByteThrowsForEof(int len)
        {
            BlockBlobClient blockBlobClient = blobContainerClient.GetBlockBlobClient("ReadByteThrowsForEof");

            using (Stream uploadStream = blockBlobClient.OpenWrite(true))
            {
                uploadStream.Write(Utils.GenerateRandomBuffer(len), 0, len);
            }


            BlobClient blobClient = blobContainerClient.GetBlobClient("ReadByteThrowsForEof");
            Stream stream = blockBlobClient.OpenRead();
            input = new AzureBlobIndexInput(blobClient, len, stream);

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