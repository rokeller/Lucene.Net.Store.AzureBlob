using System;
using System.Reflection;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Xunit.Sdk;

namespace Lucene.Net.Store
{
    [TraceRequest]
    public abstract class TestBase : IDisposable
    {
        // [ThreadStatic]
        // private static AppInsightsFixture appInsightsFixture;

        protected TestBase(AppInsightsFixture appInsightsFixture)
        {
            // AppInsightsFixture = appInsightsFixture;
            // TestBase.appInsightsFixture = appInsightsFixture;
        }

        // public AppInsightsFixture AppInsightsFixture { get; }

        public virtual void Dispose()
        { }

        // internal static AppInsightsFixture CurrentAppInsightsFixture => appInsightsFixture;
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    internal sealed class TraceRequestAttribute : BeforeAfterTestAttribute
    {
        // [ThreadStatic]
        // private static IOperationHolder<RequestTelemetry> telemetryHolder;

        public override void Before(MethodInfo methodUnderTest)
        {
            // string name = $"{methodUnderTest.DeclaringType.Name}.{methodUnderTest.Name}";
            // telemetryHolder = TestBase.CurrentAppInsightsFixture.TelemetryClient.StartOperation<RequestTelemetry>("Test | " + name);
        }

        public override void After(MethodInfo methodUnderTest)
        {
            // telemetryHolder?.Dispose();
        }
    }
}