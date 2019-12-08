using System;
using System.IO;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    internal sealed class AzureBlobIndexOutput : BufferedIndexOutput
    {
        private readonly CloudBlockBlob blob;
        private readonly Stream stream;

        private long len;

        public AzureBlobIndexOutput(CloudBlockBlob blob)
        {
            this.blob = blob;

            stream = blob.OpenWrite();
        }

        public override long Length => len;

        [Obsolete("(4.1) this method will be removed in Lucene 5.0")]
        public override void Seek(long pos)
        {
            throw new NotSupportedException();
        }

        protected override void FlushBuffer(byte[] b, int offset, int len)
        {
            stream.Write(b, offset, len);
            this.len += len;
        }

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