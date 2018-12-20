using Xunit.Abstractions;

namespace Microsoft.Extensions.Logging
{
    /// <summary>
    /// Source: https://github.com/NuGet/NuGet.Services.Metadata/blob/edde4ec5032fccb05446e79f666d30c1a0cd900e/tests/NuGet.Services.AzureSearch.Tests/Support/XunitLoggerProvider.cs
    /// </summary>
    public class XunitLoggerProvider : ILoggerProvider
    {
        private readonly ITestOutputHelper _output;
        private readonly LogLevel _minLevel;

        public XunitLoggerProvider(ITestOutputHelper output)
            : this(output, LogLevel.Trace)
        {
        }

        public XunitLoggerProvider(ITestOutputHelper output, LogLevel minLevel)
        {
            _output = output;
            _minLevel = minLevel;
        }

        public ILogger CreateLogger(string categoryName)
        {
            return new XunitLogger(_output, categoryName, _minLevel);
        }

        public void Dispose()
        {
        }
    }
}