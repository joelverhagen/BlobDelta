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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    string.Empty,
                    depth: 1);

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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    string.Empty,
                    depth: 1);

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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "b",
                    depth: 1);

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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "b",
                    depth: 1);

                Assert.Equal("b", tree.Prefix);
                Assert.Equal(2, tree.Children.Count);
                Assert.Equal("ba", tree.Children[0].Prefix);
                Assert.Equal("bb", tree.Children[1].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "ba", "bb");
                await AssertBlobNamesAt(tree.Children[1], "bb");
            }
        }

        public class SingleBlobInPrefixNotMatchingPrefix : Test
        {
            public SingleBlobInPrefixNotMatchingPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "cc", "eee", "g", "hhhh");

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "e",
                    depth: 1);

                Assert.Equal("e", tree.Prefix);
                Assert.Equal(1, tree.Children.Count);
                Assert.Equal("ee", tree.Children[0].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "eee");
            }
        }


        public class SingleBlobInPrefixMatchingPrefix : Test
        {
            public SingleBlobInPrefixMatchingPrefix(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("a", "cc", "eee");

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "cc",
                    depth: 1);

                Assert.Equal("cc", tree.Prefix);
                Assert.Empty(tree.Children);
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
                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    string.Empty,
                    depth: 1);

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
                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "a",
                    depth: 1);

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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "b",
                    depth: 1);

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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "b",
                    depth: 1);

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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "b",
                    depth: 1);

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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "😃",
                    depth: 1);

                Assert.Equal("😃", tree.Prefix);
                Assert.Equal(5, tree.Children.Count);
                Assert.Equal("😃a", tree.Children[0].Prefix);
                Assert.Equal("😃¥", tree.Children[1].Prefix);
                Assert.Equal("😃𐐷", tree.Children[2].Prefix);
                Assert.Equal("😃😃", tree.Children[3].Prefix);
                Assert.Equal("😃𤭢", tree.Children[4].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "😃a", "😃¥", "😃𐐷a", "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[1], "😃¥", "😃𐐷a", "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[2], "😃𐐷a", "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[3], "😃😃😃", "😃𤭢aa");
                await AssertBlobNamesAt(tree.Children[4], "😃𤭢aa");
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

                var tree = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    "¥",
                    depth: 1);

                Assert.Equal("¥", tree.Prefix);
                Assert.Equal(2, tree.Children.Count);
                Assert.Equal("¥a", tree.Children[0].Prefix);
                Assert.Equal("¥b", tree.Children[1].Prefix);

                await AssertBlobNamesAt(tree.Children[0], "¥a", "¥bb");
                await AssertBlobNamesAt(tree.Children[1], "¥bb");
            }
        }


        public class AllowsDrillDown : Test
        {
            public AllowsDrillDown(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("AAA", "AAAA", "AAAB", "AAB", "AAC", "ABA", "ABC", "BAA", "BCC", "CAA");

                var root = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    string.Empty,
                    depth: 1);

                var nodeA = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    root.Children[0],
                    depth: 1);

                var nodeAA = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    nodeA.Children[0],
                    depth: 1);

                var nodeAAA = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    nodeAA.Children[0],
                    depth: 1);

                var nodeAAAA = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    nodeAAA.Children[0],
                    depth: 1);

                AssertChildrenPartialPrefixes(root,    false, "A", "B", "C");
                AssertChildrenPartialPrefixes(nodeA,   false, "A", "B");
                AssertChildrenPartialPrefixes(nodeAA,  false, "A", "B", "C");
                AssertChildrenPartialPrefixes(nodeAAA,  true, "A", "B");
                AssertChildrenPartialPrefixes(nodeAAAA, true);
            }
        }

        public class CanFullyEnumerate : Test
        {
            public CanFullyEnumerate(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("AAA", "AAAA", "AAAB", "AAB", "AAC", "ABA", "ABC", "BAA", "BCC", "CAA", "CB", "D");

                var root = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    string.Empty,
                    depth: int.MaxValue);

                // Depth 0
                AssertChildrenPartialPrefixes(root, false, "A", "B", "C", "D");                            // *

                // Depth 1
                AssertChildrenPartialPrefixes(root.Children[0], false, "A", "B");                          // A*
                AssertChildrenPartialPrefixes(root.Children[1], false, "A", "C");                          // B*
                AssertChildrenPartialPrefixes(root.Children[2], false, "A", "B");                          // C*
                AssertChildrenPartialPrefixes(root.Children[3], true);                                     // D

                // Depth 2
                AssertChildrenPartialPrefixes(root.Children[0].Children[0], false, "A", "B", "C");         // AA*
                AssertChildrenPartialPrefixes(root.Children[0].Children[1], false, "A", "C");              // AB*
                AssertChildrenPartialPrefixes(root.Children[1].Children[0], false, "A");                   // BA*
                AssertChildrenPartialPrefixes(root.Children[1].Children[1], false, "C");                   // BC*
                AssertChildrenPartialPrefixes(root.Children[2].Children[0], false, "A");                   // CA*
                AssertChildrenPartialPrefixes(root.Children[2].Children[1], true);                         // CB

                // Depth 3
                AssertChildrenPartialPrefixes(root.Children[0].Children[0].Children[0], true, "A", "B");   // AAA*
                AssertChildrenPartialPrefixes(root.Children[0].Children[0].Children[1], true);             // AAB
                AssertChildrenPartialPrefixes(root.Children[0].Children[0].Children[2], true);             // AAC
                AssertChildrenPartialPrefixes(root.Children[0].Children[1].Children[0], true);             // ABA
                AssertChildrenPartialPrefixes(root.Children[0].Children[1].Children[1], true);             // ABC
                AssertChildrenPartialPrefixes(root.Children[1].Children[0].Children[0], true);             // BAA
                AssertChildrenPartialPrefixes(root.Children[1].Children[1].Children[0], true);             // BCC
                AssertChildrenPartialPrefixes(root.Children[2].Children[0].Children[0], true);             // CAA

                // Depth 4
                AssertChildrenPartialPrefixes(root.Children[0].Children[0].Children[0].Children[0], true); // AAAA
                AssertChildrenPartialPrefixes(root.Children[0].Children[0].Children[0].Children[1], true); // AAAB
            }
        }

        public class CanPartiallyEnumerate : Test
        {
            public CanPartiallyEnumerate(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                await CreateBlockBlobsAsync("AAA", "AAAA", "AAAB", "AAB", "AAC", "ABA", "ABC", "BAA", "BCC", "CAA", "CB", "D");

                var root = await Target.EnumerateLeadingCharactersAsync(
                    Account,
                    ContainerName,
                    string.Empty,
                    depth: 2);

                // Depth 0
                AssertChildrenPartialPrefixes(root, false, "A", "B", "C", "D");     // *

                // Depth 1
                AssertChildrenPartialPrefixes(root.Children[0], false, "A", "B");   // A*
                AssertChildrenPartialPrefixes(root.Children[1], false, "A", "C");   // B*
                AssertChildrenPartialPrefixes(root.Children[2], false, "A", "B");   // C*
                AssertChildrenPartialPrefixes(root.Children[3], true);              // D

                // Depth 2
                AssertChildrenPartialPrefixes(root.Children[0].Children[0], false); // AA
                AssertChildrenPartialPrefixes(root.Children[0].Children[1], false); // AB
                AssertChildrenPartialPrefixes(root.Children[1].Children[0], false); // BA
                AssertChildrenPartialPrefixes(root.Children[1].Children[1], false); // BC
                AssertChildrenPartialPrefixes(root.Children[2].Children[0], false); // CA
                AssertChildrenPartialPrefixes(root.Children[2].Children[1], false); // CB

                Assert.False(root.Children[0].Children[0].IsEnumerated);
                Assert.False(root.Children[0].Children[1].IsEnumerated);
                Assert.False(root.Children[1].Children[0].IsEnumerated);
                Assert.False(root.Children[1].Children[1].IsEnumerated);
                Assert.False(root.Children[2].Children[0].IsEnumerated);
                Assert.False(root.Children[2].Children[1].IsEnumerated);
            }
        }

        public class Test : BlobContainerEnumerableFacts.Test
        {
            public Test(ITestOutputHelper output) : base(output)
            {
                Logger = output.GetLogger<PrefixTreeBuilder>();
                Target = new PrefixTreeBuilder(
                    new PrefixTreeBuilderConfiguration(),
                    Logger);
            }

            public RecordingLogger<PrefixTreeBuilder> Logger { get; }
            public PrefixTreeBuilder Target { get; }

            public void AssertChildrenPartialPrefixes(PrefixNode node, bool isBlob, params string[] expected)
            {
                Assert.Equal(expected.Length, node.Children.Count);
                for (var i = 0; i < expected.Length; i++)
                {
                    Assert.Equal(expected[i], node.Children[i].PartialPrefix);
                }

                Assert.Equal(isBlob, node.IsBlob);
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
                    .Select(x => x.Name)
                    .ToArray();
                Assert.Equal(expected, actual);
            }
        }
    }
}
