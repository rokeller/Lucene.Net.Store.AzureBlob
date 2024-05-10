using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Lucene.Net.Index;

namespace Lucene.Net.Store
{
    /// <summary>
    /// Implements a <see cref="Directory"/> that automatically syncs directory
    /// files from Azure blobs to a local directory.
    /// </summary>
    public class FileBackedAzureBlobDirectory : AzureBlobDirectoryBase
    {
        private readonly FSDirectory fsDirectory;

        /// <summary>
        /// Initializes a new instance of <see cref="FileBackedAzureBlobDirectory"/>.
        /// </summary>
        /// <param name="fsDirectory">
        /// An instance of <see cref="FSDirectory"/> where copies of the directory
        /// files from Azure should be cached.
        /// </param>
        /// <param name="blobContainerClient">
        /// The <see cref="BlobContainerClient"/> to use to manage directory files
        /// in Azure blobs.
        /// </param>
        /// <param name="blobPrefix">
        /// The blob prefix to use for directory files.
        /// </param>
        public FileBackedAzureBlobDirectory(
            FSDirectory fsDirectory,
            BlobContainerClient blobContainerClient,
            string blobPrefix)
        : this(fsDirectory, blobContainerClient, blobPrefix, null)
        { }

        /// <summary>
        /// Initializes a new instance of <see cref="FileBackedAzureBlobDirectory"/>.
        /// </summary>
        /// <param name="fsDirectory">
        /// An instance of <see cref="FSDirectory"/> where copies of the directory
        /// files from Azure should be cached.
        /// </param>
        /// <param name="blobContainerClient">
        /// The <see cref="BlobContainerClient"/> to use to manage directory files
        /// in Azure blobs.
        /// </param>
        /// <param name="blobPrefix">
        /// The blob prefix to use for directory files.
        /// </param>
        /// <param name="options">
        /// An instance of <see cref="AzureBlobDirectoryOptions"/> that defines
        /// options for this instance.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the <paramref name="fsDirectory"/> is <c>null</c>.
        /// </exception>
        public FileBackedAzureBlobDirectory(
            FSDirectory fsDirectory,
            BlobContainerClient blobContainerClient,
            string blobPrefix,
            AzureBlobDirectoryOptions options)
        : base(blobContainerClient, blobPrefix, options)
        {
            this.fsDirectory = fsDirectory ?? throw new ArgumentNullException(nameof(fsDirectory));
        }

        /// <inheritdoc/>
        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            return fsDirectory.CreateOutput(name, context);
        }

        /// <inheritdoc/>
        public override void DeleteFile(string name)
        {
            fsDirectory.DeleteFile(name);

            // Only try to delete the corresponding blob if it's in our list of last known blobs.
            if (IsInLastKnownBlobs(name))
            {
                // Do not wait for the deletion of the blob to complete.
                DeleteBlob(name);
            }
        }

        /// <inheritdoc/>
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
                return BlobExists(name);
            }
        }

        /// <inheritdoc/>
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
                return GetBlobLength(name);
            }
        }

        /// <inheritdoc/>
        public override string[] ListAll()
        {
            HashSet<string> files = new HashSet<string>(
                Enumerable.Concat(SafeListAll(fsDirectory),
                ListBlobs().Select(ExtractBlobName)));

            return files.ToArray();
        }

        /// <inheritdoc/>
        public override IndexInput OpenInput(string name, IOContext context)
        {
#pragma warning disable 618
            if (!fsDirectory.FileExists(name))
#pragma warning restore 618
            {
                DownloadBlobToFile(name, Path.Combine(fsDirectory.Directory.FullName, name));
            }

            return fsDirectory.OpenInput(name, context);
        }

        /// <inheritdoc/>
        public override void Sync(ICollection<string> names)
        {
            fsDirectory.Sync(names);
            string rootPath = fsDirectory.Directory.FullName;

            foreach (string name in names)
            {
                BlobUploadOptions uploadOptions = null;
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

                    uploadOptions = new BlobUploadOptions()
                    {
                        Conditions = new BlobRequestConditions()
                        {
                            IfNoneMatch = ETag.All,
                        },
                    };
                }

                UploadFileAndIgnoreFailingPreconditions(name, Path.Combine(rootPath, name), uploadOptions);
            }
        }

        /// <inheritdoc/>
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

        private void UploadFileAndIgnoreFailingPreconditions(string targetName, string sourcePath, BlobUploadOptions uploadOptions = null)
        {
            try
            {
                UploadFileToBlob(targetName, sourcePath, uploadOptions);
            }
            catch (RequestFailedException ex) when (ex.Status == 409)
            {
                // Intentionally left blank -- we want to ignore this error.
            }
        }
    }
}
