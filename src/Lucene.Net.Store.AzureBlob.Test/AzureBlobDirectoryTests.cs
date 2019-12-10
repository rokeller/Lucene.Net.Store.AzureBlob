using System;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Microsoft.Azure.Storage.Blob;
using Xunit;

namespace Lucene.Net.Store
{
    public sealed class AzureBlobDirectoryTests : IDisposable
    {
        private readonly CloudBlobContainer blobContainer;
        private AzureBlobDirectory dir;

        public AzureBlobDirectoryTests()
        {
            CloudBlobClient blobClient = Utils.GetBlobClient();

            blobContainer = blobClient.GetContainerReference("azureblobdirectory-test");
            blobContainer.CreateIfNotExists();
        }

        public void Dispose()
        {
            using (dir) { }
            blobContainer.DeleteIfExists();
        }

        [Theory]
        [InlineData("segments.gen")]
        [InlineData("random")]
        public void FileExistsReturnsFalseWhenFilesDoNotExist(string name)
        {
            dir = new AzureBlobDirectory(blobContainer, "FileExistsReturnsFalseWhenFilesDoNotExist");

#pragma warning disable 618
            Assert.False(dir.FileExists(name));
#pragma warning restore 618
        }

        [Fact]
        public void FileExistsWorks()
        {
            dir = new AzureBlobDirectory(blobContainer, "FileExistsWorks");

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
            dir = new AzureBlobDirectory(blobContainer, "WriteThenReadWorks");

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
    }
}
