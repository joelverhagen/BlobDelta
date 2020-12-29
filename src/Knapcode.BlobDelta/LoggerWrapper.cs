using Microsoft.Extensions.Logging;
using System;

namespace Knapcode.BlobDelta
{
    internal class LoggerWrapper : ILogger
    {
        private readonly LogLevel _minimum;
        private readonly ILogger _inner;

        public LoggerWrapper(LogLevel minimum, ILogger logger)
        {
            _minimum = minimum;
            _inner = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return _inner.BeginScope(state);
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return logLevel >= _minimum ? _inner.IsEnabled(logLevel) : false;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            _inner.Log(logLevel, eventId, state, exception, formatter);
        }
    }
}
