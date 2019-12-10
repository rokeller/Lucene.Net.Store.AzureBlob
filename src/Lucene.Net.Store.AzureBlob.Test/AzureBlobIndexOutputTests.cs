using System;
using Microsoft.Azure.Storage.Blob;
using Xunit;

namespace Lucene.Net.Store
{
    public class AzureBlobIndexOutputTests : IDisposable
    {
        private readonly CloudBlobContainer blobContainer;
        private AzureBlobIndexOutput output;

        public AzureBlobIndexOutputTests()
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azureblobindexoutput-test");
            blobContainer.CreateIfNotExists();
        }

        public void Dispose()
        {
            using (output) { }
            blobContainer.DeleteIfExists();
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