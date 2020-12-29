using Knapcode.BlobDelta.Test.Support;
using Knapcode.Delta.Common.Test.Support;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

        public class ReturnsSameComparisonsForEqualContainers : Test
        {
            public ReturnsSameComparisonsForEqualContainers(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create();
                var blobNames = new[] { "a", "b", "c" };
                await CreateLeftBlockBlobsAsync("a", "0", "b", "1", "c", "2");
                await CreateRightBlockBlobsAsync("a", "0", "b", "1", "c", "2");

                var output = await target.ToListAsync();

                Assert.Equal(3, output.Count);
                Assert.All(output, c => Assert.Equal(BlobComparisonType.Same, c.Type));
                Assert.Equal(blobNames, output.Select(x => x.Left.Blob.Name).ToArray());
                Assert.Equal(blobNames, output.Select(x => x.Right.Blob.Name).ToArray());
            }
        }

        public class DetectsMissingFromRight : Test
        {
            public DetectsMissingFromRight(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create();
                await CreateLeftBlockBlobsAsync("a", "0", "b", "1", "c", "2");
                await CreateRightBlockBlobsAsync("a", "0", "c", "2");

                var output = await target.ToListAsync();

                Assert.Equal(3, output.Count);
                Assert.Equal(BlobComparisonType.Same, output[0].Type);
                Assert.Equal("a", output[0].Left.Blob.Name);
                Assert.Equal("a", output[0].Right.Blob.Name);
                Assert.Equal(BlobComparisonType.MissingFromRight, output[1].Type);
                Assert.Equal("b", output[1].Left.Blob.Name);
                Assert.Null(output[1].Right);
                Assert.Equal(BlobComparisonType.Same, output[2].Type);
                Assert.Equal("c", output[2].Left.Blob.Name);
                Assert.Equal("c", output[2].Right.Blob.Name);
            }
        }

        public class DetectsMissingFromLeft : Test
        {
            public DetectsMissingFromLeft(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create();
                await CreateLeftBlockBlobsAsync("a", "0", "c", "2");
                await CreateRightBlockBlobsAsync("a", "0", "b", "1", "c", "2");

                var output = await target.ToListAsync();

                Assert.Equal(3, output.Count);
                Assert.Equal(BlobComparisonType.Same, output[0].Type);
                Assert.Equal("a", output[0].Left.Blob.Name);
                Assert.Equal("a", output[0].Right.Blob.Name);
                Assert.Equal(BlobComparisonType.MissingFromLeft, output[1].Type);
                Assert.Null(output[1].Left);
                Assert.Equal("b", output[1].Right.Blob.Name);
                Assert.Equal(BlobComparisonType.Same, output[2].Type);
                Assert.Equal("c", output[2].Left.Blob.Name);
                Assert.Equal("c", output[2].Right.Blob.Name);
            }
        }

        public class DetectsMissingAndDifferentContent : Test
        {
            public DetectsMissingAndDifferentContent(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create();
                await CreateLeftBlockBlobsAsync("a", "0-0", "c", "2", "d", "3");
                await CreateRightBlockBlobsAsync("a", "0-1", "b", "1", "c", "2");

                var output = await target.ToListAsync();

                Assert.Equal(4, output.Count);
                Assert.Equal(BlobComparisonType.DifferentContent, output[0].Type);
                Assert.Equal("a", output[0].Left.Blob.Name);
                Assert.Equal("a", output[0].Right.Blob.Name);
                Assert.Equal(BlobComparisonType.MissingFromLeft, output[1].Type);
                Assert.Null(output[1].Left);
                Assert.Equal("b", output[1].Right.Blob.Name);
                Assert.Equal(BlobComparisonType.Same, output[2].Type);
                Assert.Equal("c", output[2].Left.Blob.Name);
                Assert.Equal("c", output[2].Right.Blob.Name);
                Assert.Equal(BlobComparisonType.MissingFromRight, output[3].Type);
                Assert.Equal("d", output[3].Left.Blob.Name);
                Assert.Null(output[3].Right);
            }
        }

        public class DetectsMissingContentMD5 : Test
        {
            public DetectsMissingContentMD5(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create();
                await CreateLeftBlockBlobsAsync("a", "0");
                await CreateBlockBlobAsyncWithoutContentMD5(RightContainer, "a", "1");

                var output = await target.ToListAsync();

                var comparison = Assert.Single(output);
                Assert.Equal(BlobComparisonType.MissingContentMD5, comparison.Type);
                Assert.Equal("a", comparison.Left.Blob.Name);
                Assert.Equal("a", comparison.Right.Blob.Name);
            }
        }

        public class HandlesMissingContentMD5WhenSizeIsDifferent : Test
        {
            public HandlesMissingContentMD5WhenSizeIsDifferent(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create();
                await CreateLeftBlockBlobsAsync("a", "0");
                await CreateBlockBlobAsyncWithoutContentMD5(RightContainer, "a", "00");

                var output = await target.ToListAsync();

                var comparison = Assert.Single(output);
                Assert.Equal(BlobComparisonType.DifferentContent, comparison.Type);
                Assert.Equal("a", comparison.Left.Blob.Name);
                Assert.Equal("a", comparison.Right.Blob.Name);
            }
        }

        public class HandlesDifferentBlobType : Test
        {
            public HandlesDifferentBlobType(ITestOutputHelper output) : base(output)
            {
            }

            [NoEmulatorFact]
            public async Task Run()
            {
                var target = Create();
                await CreateLeftBlockBlobsAsync("a", "0");
                await CreateAppendBlobAsync(RightContainer, "a", "0");

                var output = await target.ToListAsync();

                var comparison = Assert.Single(output);
                Assert.Equal(BlobComparisonType.DifferentBlobType, comparison.Type);
                Assert.Equal("a", comparison.Left.Blob.Name);
                Assert.Equal("a", comparison.Right.Blob.Name);
            }
        }

        public class HandlesUnsupportedBlobType : Test
        {
            public HandlesUnsupportedBlobType(ITestOutputHelper output) : base(output)
            {
            }

            [NoEmulatorFact]
            public async Task Run()
            {
                var target = Create();
                await CreateAppendBlobAsync(LeftContainer, "a", "0");
                await CreateAppendBlobAsync(RightContainer, "a", "0");

                var output = await target.ToListAsync();

                var comparison = Assert.Single(output);
                Assert.Equal(BlobComparisonType.UnsupportedBlobType, comparison.Type);
                Assert.Equal("a", comparison.Left.Blob.Name);
                Assert.Equal("a", comparison.Right.Blob.Name);
            }
        }

        public class Test : BaseBlobStorageFacts, IAsyncLifetime
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
                    new BlobEnumerable(
                        LeftContainer,
                        leftInitialContinuationToken,
                        leftPrefix,
                        leftMinBlobName,
                        leftMaxBlobName,
                        leftPageSize),
                    new BlobEnumerable(
                        RightContainer,
                        rightInitialContinuationToken,
                        rightPrefix,
                        rightMinBlobName,
                        rightMaxBlobName,
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
