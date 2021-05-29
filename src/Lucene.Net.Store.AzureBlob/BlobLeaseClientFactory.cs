using Azure.Storage.Blobs.Specialized;

namespace Lucene.Net.Store
{
    internal sealed class BlobLeaseClientFactory : IBlobLeaseClientFactory
    {
        public static readonly BlobLeaseClientFactory Default = new BlobLeaseClientFactory();

        private BlobLeaseClientFactory()
        { }

        public BlobLeaseClient GetBlobLeaseClient(BlobBaseClient blobClient)
        {
            return blobClient.GetBlobLeaseClient();
        }
    }
}