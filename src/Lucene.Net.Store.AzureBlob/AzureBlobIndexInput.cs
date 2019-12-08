using System;
using System.IO;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    internal sealed class AzureBlobIndexInput : IndexInput
    {
        private readonly CloudBlockBlob blob;
        private readonly Stream stream;

        public AzureBlobIndexInput(CloudBlockBlob blob) : base(blob.Uri.OriginalString)
        {
            this.blob = blob;
            stream = blob.OpenRead();
        }

        public override long Length => blob.Properties.Length;

        public override long GetFilePointer()
        {
            return stream.Position;
        }

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
            do
            {
                int read = stream.Read(b, offset, len);
                offset += read;
                len -= read;
            } while (len > 0);
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
            AzureBlobIndexInput clone = new AzureBlobIndexInput(blob);

            clone.stream.Seek(stream.Position, SeekOrigin.Begin);

            return clone;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                stream.Dispose();
            }
        }
    }
}