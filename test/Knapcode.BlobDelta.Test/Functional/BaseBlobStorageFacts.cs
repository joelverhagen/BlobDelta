using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Xunit.Abstractions;

namespace Knapcode.BlobDelta.Test.Functional
{
    public abstract class BaseBlobStorageFacts
    {
        static BaseBlobStorageFacts()
        {
            ServicePointManager.DefaultConnectionLimit = 64;
        }

        public ITestOutputHelper Output { get; }
        public string ConnectionString { get; }
        public CloudStorageAccount Account { get; }
        public CloudBlobClient BlobClient { get; }

        public BaseBlobStorageFacts(ITestOutputHelper output)
        {
            Output = output;
            ConnectionString = GetConnectionString();
            Account = CloudStorageAccount.Parse(ConnectionString);
            BlobClient = Account.CreateCloudBlobClient();
            Output.WriteLine($"Using blob endpoint: {Account.BlobEndpoint}");
        }

        public string ContainerNamePrefix => "blobdelta-test-";

        public string GetContainerName()
        {
            return $"{ContainerNamePrefix}{Guid.NewGuid():N}";
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

        public async Task CreateAppendBlobAsync(CloudBlobContainer container, string name, string content = "")
        {
            Output.WriteLine($"Creating append blob {name}...");
            var sw = Stopwatch.StartNew();
            await container.GetAppendBlobReference(name).UploadTextAsync(content);
            Output.WriteLine($"Done creating append blob {name} in {sw.Elapsed}.");
        }

        public async Task CreateBlockBlobAsyncWithoutContentMD5(CloudBlobContainer container, string name, string content = "")
        {
            Output.WriteLine($"Creating block blob {name} without Content-MD5...");
            var sw = Stopwatch.StartNew();
            await container.GetBlockBlobReference(name).UploadFromStreamAsync(
                new NonSeekableMemoryStream(Encoding.UTF8.GetBytes(content)),
                accessCondition: null,
                options: new BlobRequestOptions { StoreBlobContentMD5 = false },
                operationContext: null);
            Output.WriteLine($"Done creating block blob {name} without Content-MD5 in {sw.Elapsed}.");
        }

        public async Task CreateBlockBlobAsync(CloudBlobContainer container, string name, string content = "")
        {
            Output.WriteLine($"Creating block blob {name}...");
            var sw = Stopwatch.StartNew();
            await container.GetBlockBlobReference(name).UploadTextAsync(content);
            Output.WriteLine($"Done creating block blob {name} in {sw.Elapsed}.");
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

        private class NonSeekableMemoryStream : MemoryStream
        {
            public NonSeekableMemoryStream() : base()
            {
            }

            public NonSeekableMemoryStream(byte[] buffer): base(buffer)
            {
            }

            public override bool CanSeek => false;
            public override long Length => throw new NotSupportedException();
            public override long Position => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
        }
    }
}
