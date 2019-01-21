using Xunit.Abstractions;

namespace Microsoft.Extensions.Logging
{
    /// <summary>
    /// Source: https://github.com/NuGet/NuGet.Services.Metadata/blob/edde4ec5032fccb05446e79f666d30c1a0cd900e/tests/NuGet.Services.AzureSearch.Tests/Support/XunitLoggerFactoryExtensions.cs
    /// </summary>
    public static class XunitLoggerFactoryExtensions
    {
        public static ILoggerFactory AddXunit(this ILoggerFactory loggerFactory, ITestOutputHelper output)
        {
            loggerFactory.AddProvider(new XunitLoggerProvider(output));
            return loggerFactory;
        }

        public static ILoggerFactory AddXunit(this ILoggerFactory loggerFactory, ITestOutputHelper output, LogLevel minLevel)
        {
            loggerFactory.AddProvider(new XunitLoggerProvider(output, minLevel));
            return loggerFactory;
        }
    }
}