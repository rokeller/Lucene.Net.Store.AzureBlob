using System;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Xunit;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public sealed class FileBackedAzureBlobDirectoryTests : TestBase, IDisposable
    {
        private readonly BlobContainerClient blobContainerClient;
        private readonly DirectoryInfo rootDir;
        private readonly FSDirectory fsDirectory;
        private FileBackedAzureBlobDirectory dir;

        public FileBackedAzureBlobDirectoryTests(AppInsightsFixture appInsightsFixture)
        : base(appInsightsFixture)
        {
            int random = Utils.GenerateRandomInt(1000);
            rootDir = new DirectoryInfo(Path.Combine(Path.GetTempPath(), "filebackedazureblobdirectory-test-" + random));
            fsDirectory = FSDirectory.Open(rootDir);

            blobContainerClient = Utils.GetBlobContainerClient("filebackedazureblobdirectory-test-" + random);
            blobContainerClient.CreateIfNotExists();
        }

        public override void Dispose()
        {
            using (dir) { }
            blobContainerClient.DeleteIfExists();
            rootDir.Refresh();
            if (rootDir.Exists)
            {
                rootDir.Delete(recursive: true);
            }
            base.Dispose();
        }

        [Fact]
        public void CtorValidatesInput()
        {
            Assert.Throws<ArgumentNullException>("fsDirectory", () => new FileBackedAzureBlobDirectory(null, blobContainerClient, null));
            Assert.Throws<ArgumentNullException>("blobContainerClient", () => new FileBackedAzureBlobDirectory(fsDirectory, null, null));
        }

        [Fact]
        public void FileLengthReturnsCorrectLength()
        {
            BlockBlobClient blobClient = blobContainerClient.GetBlockBlobClient("sample-file");
            int len = 100 + Utils.Rng.Next(1234);
            using (Stream stream = blobClient.OpenWrite(true))
            {
                stream.Write(Utils.GenerateRandomBuffer(len), 0, len);
            }

            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, null);
            Assert.Equal((long)len, dir.FileLength("sample-file"));
        }

        [Fact]
        public void FileLengthThrowsWhenBlobDoesNotExist()
        {
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "FileLengthThrowsWhenBlobDoesNotExist");

            Assert.Throws<FileNotFoundException>(() => dir.FileLength("does-not-exist"));
        }

        [Theory]
        [InlineData("segments.gen")]
        [InlineData("random")]
        public void FileExistsReturnsFalseWhenFilesDoNotExist(string name)
        {
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "FileExistsReturnsFalseWhenFilesDoNotExist");

#pragma warning disable 618
            Assert.False(dir.FileExists(name));
#pragma warning restore 618
        }

        [Fact]
        public void FileExistsWorks()
        {
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "FileExistsWorks");

            IndexWriterConfig writerConfig = new IndexWriterConfig(Utils.Version, Utils.StandardAnalyzer)
            {
                OpenMode = OpenMode.CREATE_OR_APPEND,
            };

            using (IndexWriter writer = new IndexWriter(dir, writerConfig))
            {
                writer.AddDocument(new Document()
                {
                    new StringField("id", Utils.GenerateRandomString(10), Field.Store.YES),
                });
            }

#pragma warning disable 618
            Assert.True(dir.FileExists(IndexFileNames.SEGMENTS_GEN));

            foreach (string name in dir.ListAll())
            {
                Assert.True(dir.FileExists(name));
            }

            Assert.False(dir.FileExists("DoesNotExist"));
#pragma warning restore 618
        }

        [Fact]
        public void WriteThenReadWorks()
        {
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "WriteThenReadWorks");

            string[] ids = { Utils.GenerateRandomString(10), Utils.GenerateRandomString(10), Utils.GenerateRandomString(10), };

            IndexWriterConfig writerConfig = new IndexWriterConfig(Utils.Version, Utils.StandardAnalyzer)
            {
                OpenMode = OpenMode.CREATE,
            };

            using (IndexWriter writer = new IndexWriter(dir, writerConfig))
            {
                writer.AddDocument(new Document()
                {
                    new StringField("id", ids[0], Field.Store.YES),
                    new StringField("path", $"files:/{ids[0]}", Field.Store.YES),
                    new TextField("body", "This is the first document that is getting indexed. It does not have meaningful data, because I'm lazy. And i.e. stands for id est.", Field.Store.NO),
                });

                writer.AddDocument(new Document()
                {
                    new StringField("id", ids[1], Field.Store.YES),
                    new StringField("path", $"files:/{ids[1]}", Field.Store.YES),
                    new TextField("body", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.", Field.Store.NO),
                });

                writer.AddDocument(new Document()
                {
                    new StringField("id", ids[2], Field.Store.YES),
                    new StringField("path", $"files:/{ids[2]}", Field.Store.YES),
                    new TextField("body", "The quick brown fox jumps over the lazy dog.", Field.Store.NO),
                });
            }

            using (DirectoryReader dirReader = DirectoryReader.Open(dir))
            {
                IndexSearcher searcher = new IndexSearcher(dirReader);
                QueryParser parser = new QueryParser(Utils.Version, "body", Utils.StandardAnalyzer);
                Query query;
                TopDocs topDocs;
                ScoreDoc[] hits;
                Document doc;

                // Search for 'Data'. Only the first document should be a hit.
                query = parser.Parse("Data");

                topDocs = searcher.Search(query, 100);
                Assert.Equal(1, topDocs.TotalHits);

                hits = topDocs.ScoreDocs;
                doc = searcher.Doc(hits[0].Doc);
                Assert.Equal(ids[0], doc.Get("id"));

                // Search for 'doloR'. Only the second document should be a hit.
                query = parser.Parse("doloR");

                topDocs = searcher.Search(query, 100);
                Assert.Equal(1, topDocs.TotalHits);

                hits = topDocs.ScoreDocs;
                doc = searcher.Doc(hits[0].Doc);
                Assert.Equal(ids[1], doc.Get("id"));

                // Search for 'lAzy'. The first and third document should be hits.
                query = parser.Parse("lAzy");

                topDocs = searcher.Search(query, 100);
                Assert.Equal(2, topDocs.TotalHits);

                hits = topDocs.ScoreDocs;
                doc = searcher.Doc(hits[0].Doc);
                Assert.Equal(ids[2], doc.Get("id"));
                doc = searcher.Doc(hits[1].Doc);
                Assert.Equal(ids[0], doc.Get("id"));
            }
        }

        [Fact]
        public void CachingOfSegmentsGenWorks()
        {
            AzureBlobDirectoryOptions options = new AzureBlobDirectoryOptions()
            {
                CacheSegmentsGen = true,
            };
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "CachingOfSegmentsGenWorks", options);

            IndexWriterConfig writerConfig = new IndexWriterConfig(Utils.Version, Utils.StandardAnalyzer)
            {
                OpenMode = OpenMode.CREATE_OR_APPEND,
            };

            using (IndexWriter writer = new IndexWriter(dir, writerConfig))
            {
                writer.AddDocument(new Document()
                {
                    new StringField("id", Utils.GenerateRandomString(10), Field.Store.YES),
                });
            }

#pragma warning disable 618
            Assert.True(fsDirectory.FileExists(IndexFileNames.SEGMENTS_GEN));
            Assert.True(dir.FileExists(IndexFileNames.SEGMENTS_GEN));
#pragma warning restore 618

            // The segments.gen file should not be cached yet.
            using (DirectoryReader reader = DirectoryReader.Open(dir))
            {
                Assert.Equal(1, reader.NumDocs);
            }

            // The segments.gen file should be cached now.
            using (DirectoryReader reader = DirectoryReader.Open(dir))
            {
                Assert.Equal(1, reader.NumDocs);
            }
        }

        [Fact]
        public void OpenInputThrowsForMissingFilesOnFsDirectory()
        {
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "OpenInputThrowsForMissingFiles");

            Assert.Throws<DirectoryNotFoundException>(() => dir.OpenInput("does-not-exist", IOContext.DEFAULT));
        }

        [Fact]
        public void OpenInputThrowsForMissingFilesInAzure()
        {
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "OpenInputThrowsForMissingFiles");

            if (!fsDirectory.Directory.Exists)
            {
                fsDirectory.Directory.Create();
            }

            Assert.Throws<FileNotFoundException>(() => dir.OpenInput("does-not-exist", IOContext.DEFAULT));
        }

        [Fact]
        public void SyncDoesNotUploadFilesThatExistExceptForSegmentsGen()
        {
            List<string> filesToSync = new List<string>() { "random", IndexFileNames.SEGMENTS_GEN };
            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, "SyncDoesNotUploadFilesThatExistExceptForSegmentsGen");

            if (!fsDirectory.Directory.Exists)
            {
                fsDirectory.Directory.Create();
            }

            using (IndexOutput random = dir.CreateOutput("random", IOContext.DEFAULT))
            {
                random.WriteString("This is random v1");
            }
            using (IndexOutput segmentsGen = dir.CreateOutput(IndexFileNames.SEGMENTS_GEN, IOContext.DEFAULT))
            {
                segmentsGen.WriteString("This is segments.gen v1");
            }

            dir.Sync(filesToSync);

            using (IndexInput random = dir.OpenInput("random", IOContext.DEFAULT))
            {
                Assert.Equal("This is random v1", random.ReadString());
            }
            using (IndexInput random = dir.OpenInput(IndexFileNames.SEGMENTS_GEN, IOContext.DEFAULT))
            {
                Assert.Equal("This is segments.gen v1", random.ReadString());
            }

            // Update the files.
            using (IndexOutput random = dir.CreateOutput("random", IOContext.DEFAULT))
            {
                random.WriteString("This is random v2");
            }
            using (IndexOutput segmentsGen = dir.CreateOutput(IndexFileNames.SEGMENTS_GEN, IOContext.DEFAULT))
            {
                segmentsGen.WriteString("This is segments.gen v2");
            }

            dir.Sync(filesToSync);
            // Delete the files in the fsDirectory so they must be downloaded again.
            fsDirectory.DeleteFile("random");
            fsDirectory.DeleteFile(IndexFileNames.SEGMENTS_GEN);

            using (IndexInput random = dir.OpenInput("random", IOContext.DEFAULT))
            {
                Assert.Equal("This is random v1", random.ReadString());
            }
            using (IndexInput random = dir.OpenInput(IndexFileNames.SEGMENTS_GEN, IOContext.DEFAULT))
            {
                Assert.Equal("This is segments.gen v2", random.ReadString());
            }
        }

        [Fact]
        public void OpenInputReturnsSeekableInput()
        {
            BlockBlobClient blobClient = blobContainerClient.GetBlockBlobClient("OpenInputReturnsSeekableInput");
            int len = 32 * 1024;
            using (Stream stream = blobClient.OpenWrite(true))
            {
                Utils.WriteRepeatedly(stream, len, "DeadBeef");
            }
            rootDir.Create();

            dir = new FileBackedAzureBlobDirectory(fsDirectory, blobContainerClient, null);
            using IndexInput input = dir.OpenInput("OpenInputReturnsSeekableInput", IOContext.DEFAULT);

            const long Aligned = 0x4465616442656566;
            const long OffsetBy4 = 0x4265656644656164;

            Assert.Equal(Aligned, input.ReadInt64());
            Assert.Equal(Aligned, input.ReadInt64());

            input.Seek(4);
            Assert.Equal(OffsetBy4, input.ReadInt64());
            Assert.Equal(OffsetBy4, input.ReadInt64());

            input.Seek(20 * 1024);
            Assert.Equal(Aligned, input.ReadInt64());
            Assert.Equal(Aligned, input.ReadInt64());

            input.Seek(2 * 1024 + 4);
            Assert.Equal(OffsetBy4, input.ReadInt64());
            Assert.Equal(OffsetBy4, input.ReadInt64());
        }
    }
}
