using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Xunit.Abstractions;

namespace Knapcode.BlobDelta.Test.Functional
{
    public abstract class BaseFacts
    {
        static BaseFacts()
        {
            ServicePointManager.DefaultConnectionLimit = 64;
        }

        public ITestOutputHelper Output { get; }
        public string ConnectionString { get; }
        public CloudStorageAccount Account { get; }
        public CloudBlobClient BlobClient { get; }

        public BaseFacts(ITestOutputHelper output)
        {
            Output = output;
            ConnectionString = GetConnectionString();
            Account = CloudStorageAccount.Parse(ConnectionString);
            BlobClient = Account.CreateCloudBlobClient();
            Output.WriteLine($"Using blob endpoint: {Account.BlobEndpoint}");
        }

        public static string GetContainerName()
        {
            return $"blobdelta-test-{Guid.NewGuid():N}";
        }

        private static string GetConnectionString()
        {
            var connectionString = Environment.GetEnvironmentVariable("BLOBDELTA_STORAGE_CONNECTION_STRING");

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                connectionString = "UseDevelopmentStorage=true";
            }

            return connectionString.Trim();
        }

        public async Task CreateBlockBlobsAsync(CloudBlobContainer container, params string[] names)
        {
            var work = new ConcurrentBag<string>(names);
            var tasks = Enumerable
                .Range(0, 16)
                .Select(async _ =>
                {
                    while (work.TryTake(out var name))
                    {
                        await CreateBlockBlobAsync(container, name);
                    }
                })
                .ToList();
            await Task.WhenAll(tasks);
        }

        public async Task CreateBlockBlobAsync(CloudBlobContainer container, string name, string content = "")
        {
            Output.WriteLine($"Creating blob {name}..."); ;
            var sw = Stopwatch.StartNew();
            await container.GetBlockBlobReference(name).UploadTextAsync(content);
            Output.WriteLine($"Done creating blob {name} in {sw.Elapsed}.");
        }

        public async Task DisposeAsync(CloudBlobContainer container)
        {
            Output.WriteLine($"Deleting blob container {container.Name}...");
            var sw = Stopwatch.StartNew();
            await container.DeleteIfExistsAsync();
            Output.WriteLine($"Done deleting blob container {container.Name} in {sw.Elapsed}.");
        }

        public async Task InitializeAsync(CloudBlobContainer container)
        {
            Output.WriteLine($"Creating blob container {container.Name}...");
            var sw = Stopwatch.StartNew();
            await container.CreateIfNotExistsAsync();
            Output.WriteLine($"Done creating blob container {container.Name} in {sw.Elapsed}.");
        }
    }
}
