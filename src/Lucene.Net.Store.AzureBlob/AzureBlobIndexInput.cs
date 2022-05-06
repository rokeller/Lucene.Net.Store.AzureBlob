using System.IO;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Lucene.Net.Store
{
    internal sealed class AzureBlobIndexInput : IndexInput
    {
        private readonly BlobClient blobClient;
        private readonly BlobDownloadInfo blobDownloadInfo;
        private readonly Stream stream;

        public AzureBlobIndexInput(BlobClient blobClient, long length, Stream stream) : base(blobClient.Uri.OriginalString)
        {
            this.blobClient = blobClient;
            this.stream = stream;
            Length = length;
        }

        public AzureBlobIndexInput(BlobClient blobClient, BlobDownloadInfo blobDownloadInfo) : base(blobClient.Uri.OriginalString)
        {
            // As the blobDownloadInfo is passed to this object, its ownership is also transferred. This means that we
            // will be responsible to dispose of it when disposing of this instance.
            this.blobClient = blobClient;
            this.blobDownloadInfo = blobDownloadInfo;
            this.stream = blobDownloadInfo.Content;
            Length = blobDownloadInfo.ContentLength;
        }

        public override long Length { get; }

        public override long Position { get { return stream.Position; } }

        public override byte ReadByte()
        {
            int res = stream.ReadByte();

            if (res == -1)
            {
                throw new EndOfStreamException();
            }

            return (byte)res;
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            stream.FillBuffer(b, offset, len);
        }

        public override void Seek(long pos)
        {
            if (stream.Position != pos)
            {
                stream.Seek(pos, SeekOrigin.Begin);
            }
        }

        public override object Clone()
        {
            // TODO: Do this right: Keep track of the master input, and dispose all clones when the master is disposed.
            Stream newStream = blobClient.OpenRead();
            AzureBlobIndexInput clone = new AzureBlobIndexInput(blobClient, Length, newStream);

            clone.stream.Seek(newStream.Position, SeekOrigin.Begin);

            return clone;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                stream.Dispose();
                blobDownloadInfo?.Dispose();
            }
        }
    }
}
