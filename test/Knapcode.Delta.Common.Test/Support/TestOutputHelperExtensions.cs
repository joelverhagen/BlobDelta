using Knapcode.Delta.Common.Test.Support;
using Microsoft.Extensions.Logging;

namespace Xunit.Abstractions
{
    /// <summary>
    /// Source: https://github.com/NuGet/NuGet.Services.Metadata/blob/edde4ec5032fccb05446e79f666d30c1a0cd900e/tests/NuGet.Services.AzureSearch.Tests/Support/TestOutputHelperExtensions.cs
    /// </summary>
    public static class TestOutputHelperExtensions
    {
        public static RecordingLogger<T> GetLogger<T>(this ITestOutputHelper output)
        {
            var factory = new LoggerFactory().AddXunit(output);
            var inner = factory.CreateLogger<T>();
            return new RecordingLogger<T>(inner);
        }
    }
}
