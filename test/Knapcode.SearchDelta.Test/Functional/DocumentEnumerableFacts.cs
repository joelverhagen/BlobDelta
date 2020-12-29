using Knapcode.Delta.Common.Test.Support;
using Knapcode.SearchDelta.Test.Support;
using Microsoft.Azure.Search;
using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Knapcode.SearchDelta.Test.Functional
{
    public class DocumentEnumerableFacts : BaseSearchServiceFacts
    {
        public DocumentEnumerableFacts(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AllowsEmptyIndex()
        {
            await CreateIndexAsync<KeyedDocument>();
            var enumerable = await DocumentEnumerable.CreateAsync(
                ServiceClient,
                IndexName);

            var output = await enumerable.ToListAsync();

            Assert.Empty(output);
        }

        [Fact]
        public async Task ReturnsAllDocuments()
        {
            var keys = new[] { "A", "B", "C", "D", "E" };
            await CreateIndexAsync(GetKeyedDocuments(keys));
            var enumerable = await DocumentEnumerable.CreateAsync(
                ServiceClient,
                IndexName);

            var output = await enumerable.ToListAsync();

            Assert.Equal(keys, output.GetField(nameof(KeyedDocument.Key)));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        public async Task ObservesPageSize(int pageSize)
        {
            var keys = new[] { "A", "B", "C", "D", "E" };
            await CreateIndexAsync(GetKeyedDocuments(keys));
            var enumerable = await DocumentEnumerable.CreateAsync(
                ServiceClient,
                IndexName,
                minKey: null,
                maxKey: null,
                pageSize: pageSize);

            var output = await enumerable.ToListAsync();

            Assert.Equal(keys, output.GetField(nameof(KeyedDocument.Key)));
        }

        [Theory]
        [InlineData("B", "B C D E")]
        [InlineData("0", "A B C D E")]
        [InlineData("E", "E")]
        [InlineData("F", null)]
        public async Task ObservesMinKey(string minKey, string expected)
        {
            var keys = new[] { "A", "B", "C", "D", "E" };
            await CreateIndexAsync(GetKeyedDocuments(keys));
            var enumerable = await DocumentEnumerable.CreateAsync(
                ServiceClient,
                IndexName,
                minKey: minKey,
                maxKey: null,
                pageSize: 10);

            var output = await enumerable.ToListAsync();

            var expectedKeys = expected == null ? new string[0] : expected.Split(' ');
            Assert.Equal(expectedKeys, output.GetField(nameof(KeyedDocument.Key)));
        }

        [Theory]
        [InlineData("B", "A")]
        [InlineData("0", null)]
        [InlineData("E", "A B C D")]
        [InlineData("F", "A B C D E")]
        public async Task ObservesMaxKey(string maxKey, string expected)
        {
            var keys = new[] { "A", "B", "C", "D", "E" };
            await CreateIndexAsync(GetKeyedDocuments(keys));
            var enumerable = await DocumentEnumerable.CreateAsync(
                ServiceClient,
                IndexName,
                minKey: null,
                maxKey: maxKey,
                pageSize: 10);

            var output = await enumerable.ToListAsync();

            var expectedKeys = expected == null ? new string[0] : expected.Split(' ');
            Assert.Equal(expectedKeys, output.GetField(nameof(KeyedDocument.Key)));
        }

        [Fact]
        public async Task RejectsNonSortableKey()
        {
            await CreateIndexAsync<NonSortableKey>();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => DocumentEnumerable.CreateAsync(ServiceClient, IndexName));
            Assert.Equal(
                $"The key field 'Key' of index '{IndexName}' must be sortable.",
                ex.Message);
        }

        [Fact]
        public async Task RejectsNonFilterableKey()
        {
            await CreateIndexAsync<NonFilterableKey>();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => DocumentEnumerable.CreateAsync(ServiceClient, IndexName));
            Assert.Equal(
                $"The key field 'Key' of index '{IndexName}' must be filterable.",
                ex.Message);
        }

        public class NonSortableKey
        {
            [IsFilterable]
            [Key]
            public string Key { get; set; }
        }

        public class NonFilterableKey
        {
            [IsSortable]
            [Key]
            public string Key { get; set; }
        }
    }
}
