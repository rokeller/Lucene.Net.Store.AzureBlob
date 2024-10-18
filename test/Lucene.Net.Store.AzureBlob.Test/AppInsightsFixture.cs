using System;
using System.Threading;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.DependencyCollector;
using Microsoft.ApplicationInsights.Extensibility;
using Xunit;

namespace Lucene.Net.Store
{
    public sealed class AppInsightsFixture : IDisposable
    {
        private readonly TelemetryClient telemetryClient;
        private readonly DependencyTrackingTelemetryModule dependencyTrackingTelemetryModule;

        public AppInsightsFixture()
        {
            TelemetryConfiguration configuration = TelemetryConfiguration.CreateDefault();

            string connStr = Environment.GetEnvironmentVariable("INSTRUMENTATION_CONNECTION_STRING");
            if (String.IsNullOrWhiteSpace(connStr))
            {
                configuration.TelemetryChannel = new NullTelemetryChannel();
            }
            else
            {
                configuration.ConnectionString = connStr;
            }

            configuration.TelemetryInitializers.Add(new HttpDependenciesParsingTelemetryInitializer());

            telemetryClient = new TelemetryClient(configuration);
            dependencyTrackingTelemetryModule = InitializeDependencyTracking(configuration);

            Console.WriteLine("Telemetry is now setup for instrumentation key '{0}'.", configuration.InstrumentationKey);
            telemetryClient.TrackTrace("Telemetry is now setup", SeverityLevel.Information);
        }

        public void Dispose()
        {
            dependencyTrackingTelemetryModule.Dispose();
            Console.WriteLine("Flushing telemetry ...");
            telemetryClient.Flush();
            Thread.Sleep(TimeSpan.FromSeconds(5));
            Console.WriteLine("Finished.");
        }

        public TelemetryClient TelemetryClient => telemetryClient;
        private static DependencyTrackingTelemetryModule InitializeDependencyTracking(TelemetryConfiguration configuration)
        {
            DependencyTrackingTelemetryModule module = new DependencyTrackingTelemetryModule();

            module.Initialize(configuration);

            return module;
        }
    }

    [CollectionDefinition("AppInsights")]
    public sealed class AppInsightsCollection : ICollectionFixture<AppInsightsFixture>
    {
        // Intentionally left blank -- This is for tracking only.
    }

    internal sealed class NullTelemetryChannel : ITelemetryChannel
    {
        public bool? DeveloperMode { get; set; }
        public string EndpointAddress { get; set; }

        public void Dispose()
        {
        }

        public void Flush()
        {
        }

        public void Send(ITelemetry item)
        {
        }
    }
}
