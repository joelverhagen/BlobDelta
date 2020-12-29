using Microsoft.Extensions.Logging;
using System;

namespace Knapcode.BlobDelta
{
    public class PrefixTreeBuilderConfiguration
    {
        public PrefixTreeBuilderConfiguration() : this(16, LogLevel.Trace)
        {
        }

        public PrefixTreeBuilderConfiguration(int workerCount, LogLevel minimumLogLevel)
        {
            WorkerCount = workerCount;
            MinimumLogLevel = minimumLogLevel;

            if (workerCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(workerCount), "The worker counter must be at least 1.");
            }
        }

        public int WorkerCount { get; }
        public LogLevel MinimumLogLevel { get; }
    }
}
