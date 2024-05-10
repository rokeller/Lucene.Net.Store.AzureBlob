using System.IO;
using Azure.Storage.Blobs;

namespace Lucene.Net.Store
{
    internal sealed class AzureBlobIndexInput : IndexInput
    {
        private readonly BlobClient blobClient;
        private readonly Stream stream;

        public AzureBlobIndexInput(BlobClient blobClient, long length, Stream stream) : base(blobClient.Uri.OriginalString)
        {
            this.blobClient = blobClient;
            this.stream = stream;
            Length = length;
        }

        /// <inheritdoc/>
        public override long Length { get; }

        /// <inheritdoc/>
        public override long Position { get { return stream.Position; } }

        /// <inheritdoc/>
        public override byte ReadByte()
        {
            int res = stream.ReadByte();

            if (res == -1)
            {
                throw new EndOfStreamException();
            }

            return (byte)res;
        }

        /// <inheritdoc/>
        public override void ReadBytes(byte[] b, int offset, int len)
        {
            stream.FillBuffer(b, offset, len);
        }

        /// <inheritdoc/>
        public override void Seek(long pos)
        {
            if (stream.Position != pos)
            {
                stream.Seek(pos, SeekOrigin.Begin);
            }
        }

        /// <inheritdoc/>
        public override object Clone()
        {
            // TODO: Do this right: Keep track of the master input, and dispose all clones when the master is disposed.
            Stream newStream = blobClient.OpenRead();
            AzureBlobIndexInput clone = new AzureBlobIndexInput(blobClient, Length, newStream);

            clone.stream.Seek(newStream.Position, SeekOrigin.Begin);

            return clone;
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                stream.Dispose();
            }
        }
    }
}
