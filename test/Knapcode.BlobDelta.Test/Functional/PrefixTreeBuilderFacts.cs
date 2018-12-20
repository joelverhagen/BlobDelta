using System.Linq;
using System.Threading.Tasks;
using Knapcode.BlobDelta.Test.Support;
using Microsoft.WindowsAzure.Storage.Blob;
using Xunit;
using Xunit.Abstractions;

namespace Knapcode.BlobDelta.Test.Functional
{
    public class PrefixTreeBuilderFacts
    {
        public class EmptyInitialPrefix : Test
        {
            public EmptyInitialPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a1", "a2", "b1", "b2");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    string.Empty);

                Assert.Equal(string.Empty, tree.Prefix);
                Assert.Equal(2, tree.Children.Count);
                Assert.Equal("a", tree.Children[0].Prefix);
                Assert.Equal("b", tree.Children[1].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "a1", "a2", "b1", "b2");
                await AssertBlobNamesAt(tree.Children[1], "b1", "b2");
            }
        }

        public class DelimiterInNextBlobName : Test
        {
            public DelimiterInNextBlobName(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a1", "a2", "ba1", "ba2", "bb");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    string.Empty);

                Assert.Equal(string.Empty, tree.Prefix);
                Assert.Equal(2, tree.Children.Count);
                Assert.Equal("a", tree.Children[0].Prefix);
                Assert.Equal("b", tree.Children[1].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "a1", "a2", "ba1", "ba2", "bb");
                await AssertBlobNamesAt(tree.Children[1], "ba1", "ba2", "bb");
            }
        }

        public class NonEmptyInitialPrefix : Test
        {
            public NonEmptyInitialPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a1", "ba1", "ba2", "bb", "ca", "cb");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "b");

                Assert.Equal("b", tree.Prefix);
                Assert.Equal(2, tree.Children.Count);
                Assert.Equal("ba", tree.Children[0].Prefix);
                Assert.Equal("bb", tree.Children[1].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "ba1", "ba2", "bb");
                await AssertBlobNamesAt(tree.Children[1], "bb");
            }
        }

        public class BlobNameMatchingPrefix : Test
        {
            public BlobNameMatchingPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "b", "ba", "bb", "c");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "b");

                Assert.Equal("b", tree.Prefix);
                Assert.Equal(3, tree.Children.Count);
                Assert.Equal("b", tree.Children[0].Prefix);
                Assert.Equal("ba", tree.Children[1].Prefix);
                Assert.Equal("bb", tree.Children[2].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "b", "ba", "bb");
                await AssertBlobNamesAt(tree.Children[1], "ba", "bb");
                await AssertBlobNamesAt(tree.Children[2], "bb");
            }
        }

        public class SingleBlobInPrefix : Test
        {
            public SingleBlobInPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "cc", "eee", "g", "hhhh");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "e");

                Assert.Equal("e", tree.Prefix);
                Assert.Equal(1, tree.Children.Count);
                Assert.Equal("ee", tree.Children[0].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "eee");
            }
        }

        public class EmptyContainerWithNoPrefix : Test
        {
            public EmptyContainerWithNoPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    string.Empty);

                Assert.Equal(string.Empty, tree.Prefix);
                Assert.Empty(tree.Children);
            }
        }

        public class EmptyContainerWithPrefix : Test
        {
            public EmptyContainerWithPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "a");

                Assert.Equal("a", tree.Prefix);
                Assert.Empty(tree.Children);
            }
        }

        public class EmptyPrefix : Test
        {
            public EmptyPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "cc");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "b");

                Assert.Equal("b", tree.Prefix);
                Assert.Empty(tree.Children);
            }
        }

        public class NonSurrogateNonAsciiCharacters : Test
        {
            public NonSurrogateNonAsciiCharacters(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "ba", "b¥", "b♾a", "b惡aa", "c");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "b");

                Assert.Equal("b", tree.Prefix);
                Assert.Equal(4, tree.Children.Count);
                Assert.Equal("ba", tree.Children[0].Prefix);
                Assert.Equal("b¥", tree.Children[1].Prefix);
                Assert.Equal("b♾", tree.Children[2].Prefix);
                Assert.Equal("b惡", tree.Children[3].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "ba", "b¥", "b♾a", "b惡aa");
                await AssertBlobNamesAt(tree.Children[1], "b¥", "b♾a", "b惡aa");
                await AssertBlobNamesAt(tree.Children[2], "b♾a", "b惡aa");
                await AssertBlobNamesAt(tree.Children[3], "b惡aa");
            }
        }

        public class SurrogateCharacters : Test
        {
            public SurrogateCharacters(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "ba", "b¥", "b𐐷a", "b😃", "b𤭢aa", "c");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "b");

                Assert.Equal("b", tree.Prefix);
                Assert.Equal(5, tree.Children.Count);
                Assert.Equal("ba", tree.Children[0].Prefix);
                Assert.Equal("b¥", tree.Children[1].Prefix);
                Assert.Equal("b𐐷", tree.Children[2].Prefix);
                Assert.Equal("b😃", tree.Children[3].Prefix);
                Assert.Equal("b𤭢", tree.Children[4].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "ba", "b¥", "b𐐷a", "b😃", "b𤭢aa");
                await AssertBlobNamesAt(tree.Children[1], "b¥", "b𐐷a", "b😃", "b𤭢aa");
                await AssertBlobNamesAt(tree.Children[2], "b𐐷a", "b😃", "b𤭢aa");
                await AssertBlobNamesAt(tree.Children[3], "b😃", "b𤭢aa");
                await AssertBlobNamesAt(tree.Children[4], "b𤭢aa");
            }
        }

        public class SurrogatePrefix : Test
        {
            public SurrogatePrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "c", "😃", "😃a", "😃¥", "😃𐐷a", "😃😃😃", "😃𤭢aa", "𤭢");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "😃");

                Assert.Equal("😃", tree.Prefix);
                Assert.Equal(6, tree.Children.Count);
                Assert.Equal("😃", tree.Children[0].Prefix);
                Assert.Equal("😃a", tree.Children[1].Prefix);
                Assert.Equal("😃¥", tree.Children[2].Prefix);
                Assert.Equal("😃𐐷", tree.Children[3].Prefix);
                Assert.Equal("😃😃", tree.Children[4].Prefix);
                Assert.Equal("😃𤭢", tree.Children[5].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "😃", "😃a", "😃¥", "😃𐐷a", "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[1], "😃a", "😃¥", "😃𐐷a", "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[2], "😃¥", "😃𐐷a", "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[3], "😃𐐷a", "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[4], "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[5], "😃𤭢aa");
            }
        }

        public class NonSurrogateNonAsciiPrefix : Test
        {
            public NonSurrogateNonAsciiPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "¥", "¥a", "¥bb", "😃");

                var tree = await Target.EnumerateLeadingCharacters(
                    Account,
                    ContainerName,
                    "¥");

                Assert.Equal("¥", tree.Prefix);
                Assert.Equal(3, tree.Children.Count);
                Assert.Equal("¥", tree.Children[0].Prefix);
                Assert.Equal("¥a", tree.Children[1].Prefix);
                Assert.Equal("¥b", tree.Children[2].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "¥", "¥a", "¥bb");
                await AssertBlobNamesAt(tree.Children[1], "¥a", "¥bb");
                await AssertBlobNamesAt(tree.Children[2], "¥bb");
            }

            public class AllowsDrillDown : Test
            {
                public AllowsDrillDown(ITestOutputHelper output) : base(output)
                {
                }

                [Fact]
                public async Task Run()
                {
                    await CreateBlockBlobsAsync("AAAA", "AAAB", "AAA", "AAB", "AAC", "ABA", "ABC", "BAA", "BCC", "CAA");

                    var root = await Target.EnumerateLeadingCharacters(
                        Account,
                        ContainerName,
                        string.Empty);

                    var nodeA = await Target.EnumerateLeadingCharacters(
                        Account,
                        ContainerName,
                        root.Children[0]);

                    var nodeAA = await Target.EnumerateLeadingCharacters(
                        Account,
                        ContainerName,
                        nodeA.Children[0]);

                    var nodeAAA = await Target.EnumerateLeadingCharacters(
                        Account,
                        ContainerName,
                        nodeAA.Children[0]);

                    var nodeAAAA = await Target.EnumerateLeadingCharacters(
                        Account,
                        ContainerName,
                        nodeAAA.Children[1]);

                    AssertChildrenPartialPrefixes(root, "A", "B", "C");
                    AssertChildrenPartialPrefixes(nodeA, "A", "B");
                    AssertChildrenPartialPrefixes(nodeAA, "A", "B", "C");
                    AssertChildrenPartialPrefixes(nodeAAA, string.Empty, "A", "B");
                    AssertChildrenPartialPrefixes(nodeAAAA, string.Empty);
                }
            }
        }

        public class Test : BlobContainerEnumerableFacts.Test
        {
            public Test(ITestOutputHelper output) : base(output)
            {
                Logger = output.GetLogger<PrefixTreeBuilder>();
                Target = new PrefixTreeBuilder(Logger);
            }

            public RecordingLogger<PrefixTreeBuilder> Logger { get; }
            public PrefixTreeBuilder Target { get; }

            public void AssertChildrenPartialPrefixes(PrefixNode node, params string[] expected)
            {
                Assert.Equal(expected.Length, node.Children.Count);
                for (var i = 0; i < expected.Length; i++)
                {
                    Assert.Equal(expected[i], node.Children[i].PartialPrefix);
                }
            }

            public async Task AssertBlobNamesAt(PrefixNode node, params string[] expected)
            {
                var segment = await Container.ListBlobsSegmentedAsync(
                    prefix: node.Parent.Prefix,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.None,
                    maxResults: null,
                    currentToken: node.Token,
                    options: null,
                    operationContext: null);

                var actual = segment
                    .Results
                    .Cast<ICloudBlob>()
                    .Take(expected.Count())
                    .Select(x => x.Name)
                    .ToArray();
                Assert.Equal(expected, actual);
            }
        }
    }
}
