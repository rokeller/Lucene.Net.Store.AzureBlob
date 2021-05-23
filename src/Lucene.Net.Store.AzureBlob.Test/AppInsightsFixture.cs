using System;
using System.Threading;
using Microsoft.ApplicationInsights;
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
            string instrumentationKey = Environment.GetEnvironmentVariable("INSTRUMENTATION_KEY");
            if (String.IsNullOrWhiteSpace(instrumentationKey))
            {
                instrumentationKey = "INVALID_KEY";
            }

            TelemetryConfiguration configuration = TelemetryConfiguration.CreateDefault();
            configuration.InstrumentationKey = instrumentationKey;
            configuration.TelemetryInitializers.Add(new HttpDependenciesParsingTelemetryInitializer());

            telemetryClient = new TelemetryClient(configuration);
            dependencyTrackingTelemetryModule = InitializeDependencyTracking(configuration);

            Console.WriteLine("Telemetry is now setup for instrumentation key '{0}'.", instrumentationKey);
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
}