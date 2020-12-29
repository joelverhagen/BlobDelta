using Knapcode.Delta.Common.Test.Support;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Knapcode.TableDelta.Test.Functional
{
    public class EntityEnumerableFacts
    {
        public class ImmediatelyReturnsFalseOnEmptyContainer : Test
        {
            public ImmediatelyReturnsFalseOnEmptyContainer(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create<TestEntityA>();

                var output = await target.ToListAsync();

                Assert.Empty(output);
            }
        }

        public class ReturnsAllBlobs : Test
        {
            public ReturnsAllBlobs(ITestOutputHelper output) : base(output)
            {
            }

            [Theory]
            [InlineData(1)]
            [InlineData(3)]
            [InlineData(4)]
            [InlineData(1000)]
            public async Task Run(int takeCount)
            {
                var expected = new[]
                {
                    new TestEntityA("a", "1"),
                    new TestEntityA("a", "2"),
                    new TestEntityA("b", "1"),
                    new TestEntityA("c", "1"),
                    new TestEntityA("d", "1"),
                };
                await CreateEntitiesAsync(expected);
                var target = Create<TestEntityA>(takeCount);

                var output = await target.ToListAsync();

                Assert.Equal(expected, output.Select(x => x.Entity).ToArray());
            }
        }

        public class ContextHasCorrectProperties : Test
        {
            public ContextHasCorrectProperties(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var expected = new[]
                {
                    new TestEntityA("a", "1"),
                    new TestEntityA("a", "2"),
                    new TestEntityA("b", "1"),
                    new TestEntityA("c", "1"),
                    new TestEntityA("d", "1"),
                };
                await CreateEntitiesAsync(expected);
                var target = Create<TestEntityA>(takeCount: 2);

                var output = await target.ToListAsync();

                Assert.Equal(5, output.Count);

                Assert.Equal(expected[0], output[0].Entity);
                Assert.Equal(0, output[0].EntityIndex);
                Assert.Null(output[0].ContinuationToken);
                Assert.Equal(0, output[0].SegmentIndex);

                Assert.Equal(expected[1], output[1].Entity);
                Assert.Equal(1, output[1].EntityIndex);
                Assert.Null(output[1].ContinuationToken);
                Assert.Equal(0, output[1].SegmentIndex);

                Assert.Equal(expected[2], output[2].Entity);
                Assert.Equal(0, output[2].EntityIndex);
                Assert.NotNull(output[2].ContinuationToken);
                Assert.Equal(1, output[2].SegmentIndex);

                Assert.Equal(expected[3], output[3].Entity);
                Assert.Equal(1, output[3].EntityIndex);
                Assert.Equal(output[2].ContinuationToken, output[3].ContinuationToken);
                Assert.Equal(1, output[3].SegmentIndex);

                Assert.Equal(expected[4], output[4].Entity);
                Assert.Equal(0, output[4].EntityIndex);
                Assert.NotNull(output[4].ContinuationToken);
                Assert.NotEqual(output[3].ContinuationToken, output[4].ContinuationToken);
                Assert.Equal(2, output[4].SegmentIndex);
            }
        }

        public class Test : BaseTableStorageFacts, IAsyncLifetime
        {
            public Test(ITestOutputHelper output) : base(output)
            {
                TableName = GetTableName();
                Table = TableClient.GetTableReference(TableName);
            }

            public string TableName { get; }
            public CloudTable Table { get; }

            public async Task CreateEntitiesAsync<T>(params T[] entities) where T : ITableEntity
            {
                await CreateEntitiesAsync(Table, entities);
            }

            public EntityEnumerable<T> Create<T>(int takeCount = 1000) where T : ITableEntity
            {
                return new EntityEnumerable<T>(Table, takeCount);
            }

            public async Task DisposeAsync()
            {
                await DisposeAsync(Table);
            }

            public async Task InitializeAsync()
            {
                await InitializeAsync(Table);
            }
        }
    }
}
