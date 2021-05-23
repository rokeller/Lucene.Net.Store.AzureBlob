using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Storage.Blob;
using Xunit;
using Xunit.Abstractions;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public abstract class ConcurrencyTests : TestBase, IDisposable
    {

        private readonly CloudBlobContainer blobContainer;
        private readonly ITestOutputHelper output;
        // private readonly IOperationHolder<RequestTelemetry> telemetryHolder;
        private Directory dir1;
        private Directory dir2;

        protected ConcurrencyTests(AppInsightsFixture appInsightsFixture, ITestOutputHelper output)
        : base(appInsightsFixture)
        {
            this.output = output;

            int random = Utils.GenerateRandomInt(1000);

            blobContainer = Utils.GetBlobClient().GetContainerReference("concurrency-test-" + random);
            blobContainer.CreateIfNotExists();

            string prefix = Utils.GenerateRandomString(8);
            dir1 = GetDirectory(blobContainer.Name, prefix);
            dir2 = GetDirectory(blobContainer.Name, prefix);

            // telemetryHolder = appInsightsFixture.TelemetryClient.StartOperation<RequestTelemetry>($"Test | {GetType().Name}");
        }

        protected abstract Directory GetDirectory(string containerName, string prefix);

        public override void Dispose()
        {
            // telemetryHolder.Dispose();

            using (dir1) { }
            using (dir2) { }

            blobContainer.DeleteIfExists();
            base.Dispose();
        }

        protected async Task ConcurrentWritesArePreventedByRemoteLock()
        {
            string[] keys = { "azure-blob-dir1", "azure-blob-dir2", };

            await Task.WhenAll(
                Task.Run(() => AddDocuments(dir1, keys[0])),
                Task.Run(() => AddDocuments(dir2, keys[1])));

            QueryDocuments(dir1, keys);
            QueryDocuments(dir2, keys);
        }

        private void AddDocuments(Directory dir, string key)
        {
            IndexWriterConfig writerConfig = new IndexWriterConfig(Utils.Version, Utils.StandardAnalyzer)
            {
                OpenMode = OpenMode.CREATE_OR_APPEND,
                WriteLockTimeout = 2000, // 2sec
            };

            using (IndexWriter writer = new IndexWriter(dir, writerConfig))
            {
                writer.AddDocument(new Document()
                {
                    new StringField("id", Utils.GenerateRandomString(8), Field.Store.YES),
                    new StringField("key", key, Field.Store.YES),
                    new TextField("body", "This is the first document that is getting indexed. It does not have meaningful data, because I'm lazy. And i.e. stands for id est.", Field.Store.NO),
                });

                writer.AddDocument(new Document()
                {
                    new StringField("id", Utils.GenerateRandomString(8), Field.Store.YES),
                    new StringField("key", key, Field.Store.YES),
                    new TextField("body", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.", Field.Store.NO),
                });

                writer.AddDocument(new Document()
                {
                    new StringField("id", Utils.GenerateRandomString(8), Field.Store.YES),
                    new StringField("key", key, Field.Store.YES),
                    new TextField("body", "The quick brown fox jumps over the lazy dog.", Field.Store.NO),
                });
            }

            output.WriteLine("Written documents for key=[{0}].", key);
        }

        private static void QueryDocuments(Directory dir, string[] keys)
        {
            HashSet<string> expectedKeys = new HashSet<string>(keys);

            using (DirectoryReader reader = DirectoryReader.Open(dir))
            {
                IndexSearcher searcher = new IndexSearcher(reader);
                QueryParser parser = new QueryParser(Utils.Version, "body", Utils.StandardAnalyzer);
                Query query;
                TopDocs topDocs;
                ScoreDoc[] hits;
                Document doc;

                // Search for 'DoloR'. Only the second document should be a hit, but we should have one doc per expected key.
                query = parser.Parse("DoloR");

                topDocs = searcher.Search(query, 100);
                Assert.Equal(2, topDocs.TotalHits);

                hits = topDocs.ScoreDocs;
                for (int i = 0; i < keys.Length; i++)
                {
                    doc = searcher.Doc(hits[i].Doc);
                    Assert.True(expectedKeys.Remove(doc.Get("key")));
                }
            }
        }
    }
}