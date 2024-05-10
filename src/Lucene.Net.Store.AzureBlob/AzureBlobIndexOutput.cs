using System;
using System.IO;
using Azure.Storage.Blobs.Specialized;

namespace Lucene.Net.Store
{
    internal sealed class AzureBlobIndexOutput : BufferedIndexOutput
    {
        private readonly Stream stream;

        private long len;

        public AzureBlobIndexOutput(BlockBlobClient blobClient)
        {
            stream = blobClient.OpenWrite(overwrite: true);
        }

        /// <inheritdoc/>
        public override long Length => len;

        /// <inheritdoc/>
        [Obsolete("(4.1) this method will be removed in Lucene 5.0")]
        public override void Seek(long pos)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        protected override void FlushBuffer(byte[] b, int offset, int len)
        {
            stream.Write(b, offset, len);
            this.len += len;
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            
            if (disposing)
            {
                stream.Dispose();
            }
        }
    }
}
