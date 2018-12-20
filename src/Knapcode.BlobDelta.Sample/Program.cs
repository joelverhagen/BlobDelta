using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta.Sample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            var accountName = "explorepackages";
            var sasToken = "";
            var containerName = "packages2";
            //var accountName = "blobdelta";
            //var sasToken = "";
            //var containerName = "test";
            var account = new CloudStorageAccount(
                new StorageCredentials(sasToken),
                accountName,
                "core.windows.net",
                useHttps: true);

            await account.CreateCloudBlobClient().GetContainerReference(containerName).ExistsAsync();

        }
        private static async Task CompareAsync()
        {
            var containerLeft = GetContainer("packages");
            var containerRight = GetContainer("packages2");

            var enumerableLeft = new BlobEnumerable(containerLeft);
            var enumerableRight = new BlobEnumerable(containerRight);

            Console.WriteLine($"Left  | {containerLeft.Uri}");
            Console.WriteLine($"Right | {containerRight.Uri}");

            var blobPairEnumerable = new BlobComparisonEnumerable(
                enumerableLeft,
                enumerableRight);

            Console.WriteLine("Finding delta...");
            var enumerator = blobPairEnumerable.GetEnumerator();
            while (await enumerator.MoveNextAsync())
            {
                var paddedType = enumerator.Current.Type.ToString().PadRight(MaxTypeWidth);
                var blobName = enumerator.Current.Left?.Blob.Name ?? enumerator.Current.Right?.Blob.Name;
                Console.WriteLine($"{paddedType} | {blobName}");
            }
        }

        private static int MaxTypeWidth = Enum.GetNames(typeof(BlobComparisonType)).Max(x => x.Length);

        private static string GetConnectionString()
        {
            var connectionString = Environment.GetEnvironmentVariable("BLOBDELTA_STORAGE_CONNECTION_STRING");

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                connectionString = "UseDevelopmentStorage=true";
            }

            return connectionString.Trim();
        }

        private static CloudBlobContainer GetContainer(string containerName)
        {
            var connectionString = GetConnectionString();
            return CloudStorageAccount
                .Parse(connectionString)
                .CreateCloudBlobClient()
                .GetContainerReference(containerName);
        }
    }
}
