using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Knapcode.TableDelta.Test.Functional
{
    public abstract class BaseTableStorageFacts
    {
        public ITestOutputHelper Output { get; }
        public string ConnectionString { get; }
        public CloudStorageAccount Account { get; }
        public CloudTableClient TableClient { get; }

        public BaseTableStorageFacts(ITestOutputHelper output)
        {
            Output = output;
            ConnectionString = GetConnectionString();
            Account = CloudStorageAccount.Parse(ConnectionString);
            TableClient = Account.CreateCloudTableClient();
            Output.WriteLine($"Using table endpoint: {Account.TableEndpoint}");
        }

        public string TableNamePrefix => "tabledeltatest";

        public string GetTableName()
        {
            return $"{TableNamePrefix}{Guid.NewGuid():N}";
        }

        private static string GetConnectionString()
        {
            var connectionString = Environment.GetEnvironmentVariable("TABLEDELTA_STORAGE_CONNECTION_STRING");

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                connectionString = "UseDevelopmentStorage=true";
            }

            return connectionString.Trim();
        }

        public async Task CreateEntitiesAsync<T>(CloudTable table, params T[] entities) where T : ITableEntity
        {
            foreach (var group in entities.GroupBy(x => x.PartitionKey))
            {
                var batch = new TableBatchOperation();
                foreach (var entity in group)
                {
                    batch.Insert(entity);
                }

                await table.ExecuteBatchAsync(batch);
            }
        }

        public async Task DisposeAsync(CloudTable table)
        {
            Output.WriteLine($"Deleting table {table.Name}...");
            var sw = Stopwatch.StartNew();
            await table.DeleteIfExistsAsync();
            Output.WriteLine($"Done deleting table {table.Name} in {sw.Elapsed}.");
        }

        public async Task InitializeAsync(CloudTable table)
        {
            Output.WriteLine($"Creating table {table.Name}...");
            var sw = Stopwatch.StartNew();
            await table.CreateIfNotExistsAsync();
            Output.WriteLine($"Done creating table {table.Name} in {sw.Elapsed}.");
        }

        public class TestEntityA : TableEntity, IEquatable<TestEntityA>
        {
            public TestEntityA()
            {

            }

            public TestEntityA(string partitionKey, string rowKey) : base(partitionKey, rowKey)
            {
                StringA = partitionKey + "/" + rowKey;
            }

            public string StringA { get; set; }

            public override bool Equals(object obj)
            {
                return Equals(obj as TestEntityA);
            }

            public bool Equals(TestEntityA other)
            {
                return other != null &&
                       PartitionKey == other.PartitionKey &&
                       RowKey == other.RowKey &&
                       StringA == other.StringA;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(PartitionKey, RowKey, StringA);
            }
        }

        public class TestEntityB : TestEntityA, IEquatable<TestEntityB>
        {
            public TestEntityB(string partitionKey, string rowKey) : base(partitionKey, rowKey)
            {
                StringB = rowKey + "/" + partitionKey;
            }

            public string StringB { get; set; }

            public override bool Equals(object obj)
            {
                return Equals(obj as TestEntityB);
            }

            public bool Equals(TestEntityB other)
            {
                return other != null &&
                       base.Equals(other) &&
                       StringB == other.StringB;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(base.GetHashCode(), StringB);
            }
        }
    }
}
