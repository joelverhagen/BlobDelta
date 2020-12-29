using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Knapcode.BlobDelta.Test.Functional
{
    public class RemoveOldBlobContainers : BaseBlobStorageFacts
    {
        public RemoveOldBlobContainers(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Run()
        {
            BlobContinuationToken currentToken = null;
            var threshold = DateTimeOffset.UtcNow.AddDays(-1);
            var oldContainers = new ConcurrentBag<CloudBlobContainer>();
            do
            {
                var segment = await BlobClient.ListContainersSegmentedAsync(
                    ContainerNamePrefix,
                    currentToken: currentToken);

                foreach (var container in segment.Results)
                {
                    if (!container.Properties.LastModified.HasValue
                        || container.Properties.LastModified.Value >= threshold)
                    {
                        continue;
                    }

                    oldContainers.Add(container);
                }

                currentToken = segment.ContinuationToken;
            }
            while (currentToken != null);

            Output.WriteLine($"Found {oldContainers.Count} containers to delete.");

            var tasks = Enumerable
                .Range(0, 16)
                .Select(async _ =>
                {
                    while (oldContainers.TryTake(out var container))
                    {
                        await DisposeAsync(container);
                    }
                })
                .ToList();
            await Task.WhenAll(tasks);
        }
    }
}
