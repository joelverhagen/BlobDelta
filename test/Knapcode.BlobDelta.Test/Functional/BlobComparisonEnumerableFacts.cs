using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Xunit;
using Xunit.Abstractions;

namespace Knapcode.BlobDelta.Test.Functional
{
    public class BlobComparisonEnumerableFacts
    {
        public class ReturnsNoComparisonsFromTwoEmptyContainers : Test
        {
            public ReturnsNoComparisonsFromTwoEmptyContainers(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create();

                var output = await target.ToListAsync();

                Assert.Empty(output);
            }
        }

        public class Test : BaseFacts, IAsyncLifetime
        {
            public Test(ITestOutputHelper output) : base(output)
            {
                LeftContainerName = GetContainerName();
                RightContainerName = GetContainerName();
                LeftContainer = BlobClient.GetContainerReference(LeftContainerName);
                RightContainer = BlobClient.GetContainerReference(RightContainerName);
            }

            public string LeftContainerName { get; }
            public string RightContainerName { get; }
            public CloudBlobContainer LeftContainer { get; }
            public CloudBlobContainer RightContainer { get; }

            public BlobComparisonEnumerable Create(
                BlobContinuationToken leftInitialContinuationToken = null,
                string leftMinBlobName = null,
                string leftMaxBlobName = null,
                string leftPrefix = null,
                int leftPageSize = 5000,
                BlobContinuationToken rightInitialContinuationToken = null,
                string rightMinBlobName = null,
                string rightMaxBlobName = null,
                string rightPrefix = null,
                int rightPageSize = 5000)
            {
                return new BlobComparisonEnumerable(
                    new BlobContainerEnumerable(
                        LeftContainer,
                        leftInitialContinuationToken,
                        leftMinBlobName,
                        leftMaxBlobName,
                        leftPrefix,
                        leftPageSize),
                    new BlobContainerEnumerable(
                        RightContainer,
                        rightInitialContinuationToken,
                        rightMinBlobName,
                        rightMaxBlobName,
                        rightPrefix,
                        rightPageSize));
            }

            public async Task DisposeAsync()
            {
                await Task.WhenAll(DisposeAsync(LeftContainer), DisposeAsync(RightContainer));
            }

            public async Task InitializeAsync()
            {
                await Task.WhenAll(InitializeAsync(LeftContainer), InitializeAsync(RightContainer));
            }

            public async Task CreateLeftBlockBlobsAsync(params string[] nameAndContent)
            {
                await CreateBlockBlobsWithContentAsync(LeftContainer, nameAndContent);
            }

            public async Task CreateRightBlockBlobsAsync(params string[] nameAndContent)
            {
                await CreateBlockBlobsWithContentAsync(RightContainer, nameAndContent);
            }

            private async Task CreateBlockBlobsWithContentAsync(CloudBlobContainer container, params string[] nameAndContent)
            {
                if (nameAndContent.Length % 2 == 1)
                {
                    throw new ArgumentException("The number of strings provided must be even.");
                }

                var pairs = nameAndContent
                    .Select((x, i) => new { Value = x, Index = i })
                    .Where(x => x.Index % 2 == 0)
                    .ToDictionary(x => x.Value, x => nameAndContent[x.Index + 1]);

                var work = new ConcurrentBag<KeyValuePair<string, string>>(pairs);
                var tasks = Enumerable
                    .Range(0, 16)
                    .Select(async _ =>
                    {
                        while (work.TryTake(out var pair))
                        {
                            await CreateBlockBlobAsync(container, pair.Key, pair.Value);
                        }
                    })
                    .ToList();
                await Task.WhenAll(tasks);
            }
        }
    }
}
