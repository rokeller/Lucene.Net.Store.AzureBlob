using System;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Xunit;
using Xunit.Abstractions;

namespace Lucene.Net.Store
{
    [Collection("AppInsights")]
    public sealed class AzureBlobDirectoryConcurrencyTests : ConcurrencyTests, IDisposable
    {
        public AzureBlobDirectoryConcurrencyTests(AppInsightsFixture appInsightsFixture, ITestOutputHelper output)
        : base(appInsightsFixture, output)
        { }

        public override void Dispose()
        {
            base.Dispose();
        }

        protected override Directory GetDirectory(string containerName, string prefix)
        {
            BlobContainerClient containerClient = Utils.GetBlobContainerClient(containerName);

            return new AzureBlobDirectory(containerClient, prefix);
        }

        [Fact]
        public async Task Test()
        {
            using IOperationHolder<RequestTelemetry> telemetry =
                AppInsightsFixture.TelemetryClient.StartOperation<RequestTelemetry>($"Test | AzureBlobDirectoryConcurrencyTests.Test");

            await ConcurrentWritesArePreventedByRemoteLock();
        }
    }
}