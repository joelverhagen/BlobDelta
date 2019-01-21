using System;
using System.Linq;
using Xunit.Abstractions;

namespace Microsoft.Extensions.Logging
{
    /// <summary>
    /// Source: https://github.com/NuGet/NuGet.Services.Metadata/blob/edde4ec5032fccb05446e79f666d30c1a0cd900e/tests/NuGet.Services.AzureSearch.Tests/Support/XunitLogger.cs
    /// </summary>
    public class XunitLogger : ILogger
    {
        private static readonly char[] NewLineChars = new[] { '\r', '\n' };
        private readonly string _category;
        private readonly LogLevel _minLogLevel;
        private readonly ITestOutputHelper _output;

        public XunitLogger(ITestOutputHelper output, string category, LogLevel minLogLevel)
        {
            _minLogLevel = minLogLevel;
            _category = category;
            _output = output;
        }

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }
            var firstLinePrefix = $"| {_category} {logLevel}: ";
            var message = formatter(state, exception);
            if (exception != null)
            {
                message += Environment.NewLine + exception.ToString();
            }
            var lines = message.Split('\n');
            _output.WriteLine(firstLinePrefix + lines.First().TrimEnd(NewLineChars));

            var additionalLinePrefix = "|" + new string(' ', firstLinePrefix.Length - 1);
            foreach (var line in lines.Skip(1))
            {
                _output.WriteLine(additionalLinePrefix + line.TrimEnd(NewLineChars));
            }
        }

        public bool IsEnabled(LogLevel logLevel)
            => logLevel >= _minLogLevel;

        public IDisposable BeginScope<TState>(TState state)
            => new NullScope();

        private class NullScope : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}