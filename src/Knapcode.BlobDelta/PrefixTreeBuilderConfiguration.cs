using System;
using Microsoft.Extensions.Logging;

namespace Knapcode.BlobDelta
{
    public class PrefixTreeBuilderConfiguration
    {
        public PrefixTreeBuilderConfiguration() : this(LogLevel.Trace)
        {
        }

        public PrefixTreeBuilderConfiguration(LogLevel minimumLogLevel)
        {
            MinimumLogLevel = minimumLogLevel;
        }

        public LogLevel MinimumLogLevel { get; }
    }
}
