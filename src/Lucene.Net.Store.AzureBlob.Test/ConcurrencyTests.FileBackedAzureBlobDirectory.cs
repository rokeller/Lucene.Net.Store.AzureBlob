using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Storage.Blob;
using Xunit.Abstractions;
using Xunit;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public sealed class FileBackedAzureBlobDirectoryConcurrencyTests : ConcurrencyTests, IDisposable
    {
        private readonly DirectoryInfo rootDir = new DirectoryInfo(Path.Combine(Path.GetTempPath(), "concurrency-tests", "file-backed"));

        private readonly List<FSDirectory> fsDirectories = new List<FSDirectory>();
        private int localDirectoryIndex = 0;

        public FileBackedAzureBlobDirectoryConcurrencyTests(AppInsightsFixture appInsightsFixture, ITestOutputHelper output)
        : base(appInsightsFixture, output)
        { }

        public override void Dispose()
        {
            base.Dispose();

            foreach (FSDirectory fsDir in fsDirectories)
            {
                fsDir.Dispose();
            }

            rootDir.Refresh();
            if (rootDir.Exists)
            {
                rootDir.Delete(recursive: true);
            }
        }

        protected override Directory GetDirectory(string containerName, string prefix)
        {
            CloudBlobContainer container = Utils.GetBlobClient().GetContainerReference(containerName);
            DirectoryInfo dir = new DirectoryInfo(Path.Combine(rootDir.FullName, prefix, Interlocked.Increment(ref localDirectoryIndex).ToString()));
            dir.Create();
            FSDirectory fsDirectory = FSDirectory.Open(dir);
            fsDirectories.Add(fsDirectory);

            return new FileBackedAzureBlobDirectory(fsDirectory, container, prefix);
        }

        [Fact]
        public async Task Test()
        {
            using IOperationHolder<RequestTelemetry> telemetry =
                AppInsightsFixture.TelemetryClient.StartOperation<RequestTelemetry>($"Test | FileBackedAzureBlobDirectoryConcurrencyTests.Test");

            await ConcurrentWritesArePreventedByRemoteLock();
        }
    }
}
