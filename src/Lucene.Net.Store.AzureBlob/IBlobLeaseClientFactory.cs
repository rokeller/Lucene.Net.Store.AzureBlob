using Azure.Storage.Blobs.Specialized;

namespace Lucene.Net.Store
{
    internal interface IBlobLeaseClientFactory
    {
        BlobLeaseClient GetBlobLeaseClient(BlobBaseClient blobClient);
    }
}