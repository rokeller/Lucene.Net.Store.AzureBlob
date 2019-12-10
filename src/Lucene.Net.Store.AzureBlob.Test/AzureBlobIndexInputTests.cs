using System;
using System.IO;
using Microsoft.Azure.Storage.Blob;
using Xunit;

namespace Lucene.Net.Store
{
    public sealed class AzureBlobIndexInputTests : IDisposable
    {
        private readonly CloudBlobContainer blobContainer;
        private AzureBlobIndexInput input;

        public AzureBlobIndexInputTests()
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azureblobindexinput-test");
            blobContainer.CreateIfNotExists();
        }

        public void Dispose()
        {
            using (input) { }
            blobContainer.DeleteIfExists();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(137)]
        [InlineData(16 * 1024 + 17)]
        public void ReadByteThrowsForEof(int len)
        {
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference("ReadByteThrowsForEof");

            blob.UploadFromByteArray(Utils.GenerateRandomBuffer(len), 0, len);

            input = new AzureBlobIndexInput(blob);

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