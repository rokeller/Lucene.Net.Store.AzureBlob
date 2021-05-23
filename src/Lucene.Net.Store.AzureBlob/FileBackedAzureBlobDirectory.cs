using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Lucene.Net.Index;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Lucene.Net.Store
{
    public class FileBackedAzureBlobDirectory : AzureBlobDirectoryBase
    {
        private readonly FSDirectory fsDirectory;

        public FileBackedAzureBlobDirectory(FSDirectory fsDirectory, CloudBlobContainer blobContainer, string blobPrefix)
        : this(fsDirectory, blobContainer, blobPrefix, null)
        { }

        public FileBackedAzureBlobDirectory(FSDirectory fsDirectory, CloudBlobContainer blobContainer, string blobPrefix, AzureBlobDirectoryOptions options)
        : base(blobContainer, blobPrefix, options)
        {
            this.fsDirectory = fsDirectory ?? throw new ArgumentNullException(nameof(fsDirectory));
        }

        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            return fsDirectory.CreateOutput(name, context);
        }

        public override void DeleteFile(string name)
        {
            fsDirectory.DeleteFile(name);

            // Only try to delete the corresponding blob if it's in our list of last known blobs.
            if (IsInLastKnownBlobs(name))
            {
                // Do not wait for the deletion of the blob to complete.
                DeleteFileAsync(name).Ignore();
            }
        }

        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name)
        {
#pragma warning disable 618
            if (fsDirectory.FileExists(name))
#pragma warning restore 618
            {
                return true;
            }
            else
            {
                return FileExistsAsync(name).SafeWait();
            }
        }

        public override long FileLength(string name)
        {
#pragma warning disable 618
            if (fsDirectory.FileExists(name))
#pragma warning restore 618
            {
                return fsDirectory.FileLength(name);
            }
            else
            {
                return GetFileLengthAsync(name).SafeWait();
            }
        }

        public override string[] ListAll()
        {
            HashSet<string> files = new HashSet<string>(
                Enumerable.Concat(SafeListAll(fsDirectory),
                ListBlobs().Select(ExtractBlobName)));

            return files.ToArray();
        }

        public override IndexInput OpenInput(string name, IOContext context)
        {
#pragma warning disable 618
            if (!fsDirectory.FileExists(name))
#pragma warning restore 618
            {
                DownloadFileAsync(name, Path.Combine(fsDirectory.Directory.FullName, name)).SafeWait();
            }

            return fsDirectory.OpenInput(name, context);
        }

        public override void Sync(ICollection<string> names)
        {
            fsDirectory.Sync(names);
            List<Task> pending = new List<Task>(names.Count);
            string rootPath = fsDirectory.Directory.FullName;

            foreach (string name in names)
            {
                AccessCondition accessCondition = null;
                if (!StringComparer.Ordinal.Equals(name, IndexFileNames.SEGMENTS_GEN))
                {
                    // Files other than the "segments.gen" (which we always upload), we want to upload only when they
                    // do not exist in blob storage yet. By checking against the last known blobs and by specifying the
                    // wildcard ETag '*' on the 'If-None-Match' request header as a secondary barrier, we get this exact
                    // behavior.
                    // See https://docs.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations#Subheading1
                    if (IsInLastKnownBlobs(name))
                    {
                        continue;
                    }

                    accessCondition = new AccessCondition()
                    {
                        IfNoneMatchETag = "*",
                    };
                }

                pending.Add(
                    UploadFileAndIgnoreFailingPrecodintionsAsync(name, Path.Combine(rootPath, name), accessCondition));
            }

            Task.WhenAll(pending).SafeWait();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                fsDirectory.Dispose();
            }
        }

        private static string[] SafeListAll(Directory directory)
        {
            try
            {
                return directory.ListAll();
            }
            catch (DirectoryNotFoundException)
            {
                return new string[0];
            }
        }

        private async Task UploadFileAndIgnoreFailingPrecodintionsAsync(string targetName, string sourcePath, AccessCondition accessCondition = null)
        {
            try
            {
                await UploadFileAsync(targetName, sourcePath, accessCondition);
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == 409)
            {
                // Intentionally left blank -- we want to ignore this error.
            }
        }
    }
}