using Knapcode.Delta.Common.Test.Support;
using Knapcode.TableDelta;
using Knapcode.TableDelta.Test.Functional;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Knapcode.BlobDelta.Test.Functional
{
    public class TableComparisonEnumerableFacts
    {
        public class ReturnsNoComparisonsFromTwoEmptyTables : Test
        {
            public ReturnsNoComparisonsFromTwoEmptyTables(ITestOutputHelper output) : base(output)
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

        public class ReturnsSameComparisonsForEqualTables : Test
        {
            public ReturnsSameComparisonsForEqualTables(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create<TestEntityA>();
                var entities = new[]
                {
                    new TestEntityA("a", "0"),
                    new TestEntityA("a", "1"),
                    new TestEntityA("b", "2"),
                };
                await CreateLeftEntitiesAsync(entities);
                await CreateRightEntitiesAsync(entities);

                var output = await target.ToListAsync();

                Assert.Equal(3, output.Count);
                Assert.All(output, c => Assert.Equal(EntityComparisonType.Same, c.Type));
                Assert.Equal(entities.ToArray(), output.Select(x => x.Left.Entity).ToArray());
                Assert.Equal(entities.ToArray(), output.Select(x => x.Right.Entity).ToArray());
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
                var target = Create<TestEntityA>();
                var a0 = new TestEntityA("a", "0");
                var a1 = new TestEntityA("a", "1");
                var b2 = new TestEntityA("b", "2");
                await CreateLeftEntitiesAsync(a0, a1, b2);
                await CreateRightEntitiesAsync(a0, b2);

                var output = await target.ToListAsync();

                Assert.Equal(3, output.Count);
                Assert.Equal(EntityComparisonType.Same, output[0].Type);
                Assert.Equal(a0, output[0].Left.Entity);
                Assert.Equal(a0, output[0].Right.Entity);
                Assert.Equal(EntityComparisonType.MissingFromRight, output[1].Type);
                Assert.Equal(a1, output[1].Left.Entity);
                Assert.Null(output[1].Right);
                Assert.Equal(EntityComparisonType.Same, output[2].Type);
                Assert.Equal(b2, output[2].Left.Entity);
                Assert.Equal(b2, output[2].Right.Entity);
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
                var target = Create<TestEntityA>();
                var a0 = new TestEntityA("a", "0");
                var a1 = new TestEntityA("a", "1");
                var b2 = new TestEntityA("b", "2");
                await CreateLeftEntitiesAsync(a0, b2);
                await CreateRightEntitiesAsync(a0, a1, b2);

                var output = await target.ToListAsync();

                Assert.Equal(3, output.Count);
                Assert.Equal(EntityComparisonType.Same, output[0].Type);
                Assert.Equal(a0, output[0].Left.Entity);
                Assert.Equal(a0, output[0].Right.Entity);
                Assert.Equal(EntityComparisonType.MissingFromLeft, output[1].Type);
                Assert.Null(output[1].Left);
                Assert.Equal(a1, output[1].Right.Entity);
                Assert.Equal(EntityComparisonType.Same, output[2].Type);
                Assert.Equal(b2, output[2].Left.Entity);
                Assert.Equal(b2, output[2].Right.Entity);
            }
        }

        public class DetectsMissingAndDifferentPropertyValues : Test
        {
            public DetectsMissingAndDifferentPropertyValues(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create<TestEntityA>();
                var a0L = new TestEntityA("a", "0") { StringA = "L" };
                var a0R = new TestEntityA("a", "0") { StringA = "R" };
                var b1 = new TestEntityA("b", "1");
                var c2 = new TestEntityA("c", "2");
                var d3 = new TestEntityA("d", "3");
                await CreateLeftEntitiesAsync(a0L, c2, d3);
                await CreateRightEntitiesAsync(a0R, b1, c2);

                var output = await target.ToListAsync();

                Assert.Equal(4, output.Count);
                Assert.Equal(EntityComparisonType.DifferentPropertiesValues, output[0].Type);
                Assert.Equal(a0L, output[0].Left.Entity);
                Assert.Equal(a0R, output[0].Right.Entity);
                Assert.Equal(EntityComparisonType.MissingFromLeft, output[1].Type);
                Assert.Null(output[1].Left);
                Assert.Equal(b1, output[1].Right.Entity);
                Assert.Equal(EntityComparisonType.Same, output[2].Type);
                Assert.Equal(c2, output[2].Left.Entity);
                Assert.Equal(c2, output[2].Right.Entity);
                Assert.Equal(EntityComparisonType.MissingFromRight, output[3].Type);
                Assert.Equal(d3, output[3].Left.Entity);
                Assert.Null(output[3].Right);
            }
        }

        public class DetectsDisjointProperties : Test
        {
            public DetectsDisjointProperties(ITestOutputHelper output) : base(output)
            {
            }

            [Fact]
            public async Task Run()
            {
                var target = Create<TestEntityA>();
                var a1 = new TestEntityA("a", "1");
                var b1L = new TestEntityB("b", "1");
                var b1R = new TestEntityA("b", "1");
                var c1 = new TestEntityA("c", "1");
                await CreateLeftEntitiesAsync(a1, c1);
                await CreateLeftEntitiesAsync(b1L); // This one is a different type with an additional property
                await CreateRightEntitiesAsync(a1, b1R, c1);

                var output = await target.ToListAsync();

                Assert.Equal(3, output.Count);
                Assert.Equal(EntityComparisonType.Same, output[0].Type);
                Assert.Equal(a1, output[0].Left.Entity);
                Assert.Equal(a1, output[0].Right.Entity);
                Assert.Equal(EntityComparisonType.DisjointProperties, output[1].Type);
                Assert.Equal(b1L, output[1].Left.Entity);
                Assert.Equal(b1R, output[1].Right.Entity);
                Assert.Equal(EntityComparisonType.Same, output[2].Type);
                Assert.Equal(c1, output[2].Left.Entity);
                Assert.Equal(c1, output[2].Right.Entity);
            }
        }

        public class Test : BaseTableStorageFacts, IAsyncLifetime
        {
            public Test(ITestOutputHelper output) : base(output)
            {
                LeftTableName = GetTableName();
                RightTableName = GetTableName();
                LeftTable = TableClient.GetTableReference(LeftTableName);
                RightTable = TableClient.GetTableReference(RightTableName);
            }

            public string LeftTableName { get; }
            public string RightTableName { get; }
            public CloudTable LeftTable { get; }
            public CloudTable RightTable { get; }

            public EntityComparisonEnumerable<T> Create<T>(
                int leftTakeCount = 1000,
                int rightTakeCount = 1000) where T : ITableEntity, new()
            {
                return new EntityComparisonEnumerable<T>(
                    new EntityEnumerable<T>(LeftTable, leftTakeCount),
                    new EntityEnumerable<T>(RightTable, rightTakeCount));
            }

            public async Task CreateLeftEntitiesAsync<T>(params T[] entities) where T : ITableEntity
            {
                await CreateEntitiesAsync(LeftTable, entities);
            }

            public async Task CreateRightEntitiesAsync<T>(params T[] entities) where T : ITableEntity
            {
                await CreateEntitiesAsync(RightTable, entities);
            }

            public async Task DisposeAsync()
            {
                await Task.WhenAll(DisposeAsync(LeftTable), DisposeAsync(RightTable));
            }

            public async Task InitializeAsync()
            {
                await Task.WhenAll(InitializeAsync(LeftTable), InitializeAsync(RightTable));
            }
        }
    }
}
