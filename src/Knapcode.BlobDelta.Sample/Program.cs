using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta.Sample
{

    public class Program
    {
        private const int PrefixPad = 0;
        private const int DelimiterPad = 0;

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

            var node = new Node(parent: null, partialPrefix: "");
            await EnumerateLeadingCharacters(account, containerName, node, maxResults: 20, flatSegmentBlobs: null);
        }

        private static async Task EnumerateLeadingCharacters(
            CloudStorageAccount account,
            string containerName,
            Node node,
            int maxResults,
            List<ICloudBlob> flatSegmentBlobs)
        {
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            string delimiter = null;
            if (flatSegmentBlobs == null)
            {
                var flatSegment = await GetFlatSegmentAsync(container, node.Prefix, delimiter, maxResults);
                flatSegmentBlobs = flatSegment.Results.Cast<ICloudBlob>().ToList();

                // If we have a partial page, that means out prefix is specific enough to not need to seek out more
                // specific prefixes or adjacent prefixes.
                if (flatSegmentBlobs.Count < maxResults)
                {
                    Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | DONE");
                    return;
                }

                var flatSegmentBlobNames = flatSegmentBlobs.Select(x => x.Name).ToList();

                // Seek out a more specific prefix, is available. We do this based off of the limited knowledge of the page
                // we just fetched.
                var longestPrefix = GetLongestSharedPrefix(flatSegmentBlobNames, node.Prefix.Length);
                Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | LONGEST PREFIX '{longestPrefix}'");

                var lastUnsharedPrefix = GetLastUnsharedPrefix(flatSegmentBlobNames, node.Prefix.Length);
                Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | LAST UNSHARED PREFIX '{lastUnsharedPrefix}'");

                if (longestPrefix.Length > 0)
                {
                    var childNode = node.GetOrAddChild(longestPrefix);
                    await EnumerateLeadingCharacters(
                        account,
                        containerName,
                        childNode,
                        maxResults,
                        flatSegmentBlobs);
                }
            }

            var firstBlobName = await GetFirstBlobNameAsync(container, prefix, maxPageSize);
            delimiter = GetNthCharacter(firstBlobName, node.Prefix.Length);
            PrintDelimiter(node.Prefix, delimiter);

            // string delimiter = null;
            BlobContinuationToken currentToken = null;
            while (true)
            {
                // TODO: it's possible that we have already fetched a superset of the flat segment blobs above.
                var childNode = node.GetOrAddChild(delimiter);
                await EnumerateLeadingCharacters(
                    account,
                    containerName,
                    childNode,
                    maxResults,
                    flatSegmentBlobs: null);

                var segment = await GetDelimitedSegmentAsync(
                    account,
                    containerName,
                    node.Prefix,
                    delimiter,
                    2,
                    currentToken);
                var results = segment.Results.ToList();
                if (results.Count > 1)
                {
                    currentToken = await GetDelimitedContinuationTokenAsync(
                        account,
                        containerName,
                        node.Prefix,
                        delimiter,
                        currentToken);

                    var second = results[1];
                    if (second is CloudBlobDirectory nextDirectory)
                    {
                        delimiter = GetNthCharacter(nextDirectory.Prefix, node.Prefix.Length);
                    }
                    else
                    {
                        var nextBlob = (ICloudBlob)results[1];
                        delimiter = GetNthCharacter(nextBlob.Name, node.Prefix.Length);
                    }

                    PrintDelimiter(node.Prefix, delimiter);
                }
                else
                {
                    break;
                }
            }

            Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | DONE");
        }

        private static async Task EnumeratePrefixes(
            CloudStorageAccount account,
            string containerName,
            Node node,
            int maxResults,
            List<ICloudBlob> flatSegmentBlobs)
        {
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            string delimiter = null;
            if (flatSegmentBlobs == null)
            {
                var flatSegment = await GetFlatSegmentAsync(container, node.Prefix, delimiter, maxResults);
                flatSegmentBlobs = flatSegment.Results.Cast<ICloudBlob>().ToList();

                // If we have a partial page, that means out prefix is specific enough to not need to seek out more
                // specific prefixes or adjacent prefixes.
                if (flatSegmentBlobs.Count < maxResults)
                {
                    Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | DONE");
                    return;
                }

                var flatSegmentBlobNames = flatSegmentBlobs.Select(x => x.Name).ToList();

                // Seek out a more specific prefix, is available. We do this based off of the limited knowledge of the page
                // we just fetched.
                var longestPrefix = GetLongestSharedPrefix(flatSegmentBlobNames, node.Prefix.Length);
                Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | LONGEST PREFIX '{longestPrefix}'");

                var lastUnsharedPrefix = GetLastUnsharedPrefix(flatSegmentBlobNames, node.Prefix.Length);
                Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | LAST UNSHARED PREFIX '{lastUnsharedPrefix}'");

                if (longestPrefix.Length > 0)
                {
                    var childNode = node.GetOrAddChild(longestPrefix);
                    await EnumerateLeadingCharacters(
                        account,
                        containerName,
                        childNode,
                        maxResults,
                        flatSegmentBlobs);
                }
            }

            var firstBlobName = flatSegmentBlobs.First().Name;
            delimiter = GetNthCharacter(firstBlobName, node.Prefix.Length);
            PrintDelimiter(node.Prefix, delimiter);

            //var firstBlobName = await GetFirstBlobNameAsync(container, prefix, maxPageSize);
            //if (prefixLength >= firstBlobName.Length)
            //{
            //    return;
            //}
            //var delimiter = GetNthCharacter(firstBlobName, prefixLength);
            //PrintDelimiter(prefix, delimiter);
            //await EnumerateLeadingCharacters(account, containerName, prefix + delimiter, prefixLength + delimiter.Length, maxPageSize);

            // string delimiter = null;
            BlobContinuationToken currentToken = null;
            while (true)
            {
                // TODO: it's possible that we have already fetched a superset of the flat segment blobs above.
                var childNode = node.GetOrAddChild(delimiter);
                await EnumerateLeadingCharacters(
                    account,
                    containerName,
                    childNode,
                    maxResults,
                    flatSegmentBlobs: null);

                var segment = await GetDelimitedSegmentAsync(
                    account,
                    containerName,
                    node.Prefix,
                    delimiter,
                    2,
                    currentToken);
                var results = segment.Results.ToList();
                if (results.Count > 1)
                {
                    currentToken = await GetDelimitedContinuationTokenAsync(
                        account,
                        containerName,
                        node.Prefix,
                        delimiter,
                        currentToken);

                    var second = results[1];
                    if (second is CloudBlobDirectory nextDirectory)
                    {
                        delimiter = GetNthCharacter(nextDirectory.Prefix, node.Prefix.Length);
                    }
                    else
                    {
                        var nextBlob = (ICloudBlob)results[1];
                        delimiter = GetNthCharacter(nextBlob.Name, node.Prefix.Length);
                    }

                    PrintDelimiter(node.Prefix, delimiter);
                }
                else
                {
                    break;
                }
            }

            Console.WriteLine($"{node.Prefix,PrefixPad} | {delimiter,DelimiterPad} | DONE");
        }

        private static async Task<BlobContinuationToken> GetDelimitedContinuationTokenAsync(
            CloudStorageAccount account,
            string containerName,
            string prefix,
            string delimiter,
            BlobContinuationToken currentToken)
        {
            Console.WriteLine($"{prefix,PrefixPad} | {delimiter,DelimiterPad} | TOKEN AFTER {delimiter}");

            var segment = await GetDelimitedSegmentInternalAsync(
                account,
                containerName,
                prefix,
                delimiter,
                maxResults: 1,
                currentToken: currentToken);

            return segment.ContinuationToken;
        }

        private static string GetLastUnsharedPrefix(IReadOnlyList<string> input, int startIndex)
        {
            if (input == null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            if (input.Count == 0)
            {
                throw new ArgumentException("There must be at least one string in the input list.", nameof(input));
            }

            // Verify the list is sorted and unique.
            for (var i = 1; i < input.Count; i++)
            {
                if (string.CompareOrdinal(input[i - 1], input[i]) >= 0)
                {
                    throw new ArgumentException("The list must have unique items and must be sorted ordinally, ascending.", nameof(input));
                }
            }

            var last = input.Last();

            // Verify the characters before the start index are the same and that the start index is valid.
            for (var otherIndex = 0; otherIndex < input.Count - 1; otherIndex++)
            {
                var other = input[otherIndex];
                if (startIndex >= other.Length)
                {
                    throw new ArgumentException("The start index must be valid for all input strings.", nameof(input));
                }

                for (var i = 0; i < startIndex; i++)
                {
                    if (other[i] != last[i])
                    {
                        throw new ArgumentException("All of the characters up to the start index must be the same in all strings.", nameof(input));
                    }
                }
            }

            for (var i = startIndex; i < last.Length; i++)
            {
                var allMatching = true;
                string other = null;
                for (var otherIndex = input.Count - 2; otherIndex >= 0; otherIndex--)
                {
                    other = input[otherIndex];
                    // TODO: handle i being out of bounds in other
                    if (last[i] != other[i])
                    {
                        allMatching = false;
                        break;
                    }
                }

                if (!allMatching)
                {
                    // Consider the entire string to be the last prefix. This is correct because we use this as an
                    // upper bound of known prefix ranges. The prefix (substring) shared with this string and the last
                    // string could be considered an upper bound but that upper bound actually is less than the whole
                    // string when sorting ordinally.
                    // return other;

                    // TODO: figure out which one is right
                    return other.Substring(startIndex, (i - startIndex) + 1);
                }
            }

            return null;
        }

        private static string GetLongestSharedPrefix(IReadOnlyList<string> input, int startIndex)
        {
            if (input == null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            if (input.Count == 0)
            {
                throw new ArgumentException("There must be at least one string in the input list.", nameof(input));
            }

            var first = input[0];
            if (input.Count == 1)
            {
                return first;
            }

            for (var candidateLength = input[0].Length; candidateLength > startIndex; candidateLength--)
            {
                var allMatching = true;
                for (var otherIndex = 1; otherIndex < input.Count; otherIndex++)
                {
                    if (string.CompareOrdinal(first, startIndex, input[otherIndex], startIndex, candidateLength - startIndex) != 0)
                    {
                        allMatching = false;
                        break;
                    }
                }

                if (allMatching)
                {
                    // Don't split surrogate pairs.
                    if (char.IsHighSurrogate(first[candidateLength - 1]))
                    {
                        return first.Substring(startIndex, (candidateLength - startIndex) - 1);
                    }
                    else
                    {
                        return first.Substring(startIndex, candidateLength - startIndex);
                    }
                }
            }

            return string.Empty;
        }

        private static void PrintDelimiter(string prefix, string delimiter)
        {
            Console.WriteLine($"{prefix,PrefixPad} | {delimiter,DelimiterPad} | NEW DELIMITER");
        }

        private static async Task<string> GetFirstBlobNameAsync(
            CloudBlobContainer container,
            string prefix,
            string delimiter)
        {
            Console.WriteLine($"{prefix,PrefixPad} | {delimiter,DelimiterPad} | FIRST BLOB");

            GetFlatSegmentAsync(
                container,
                prefix,)
        }

        private static async Task<BlobResultSegment> GetDelimitedSegmentAsync(
            CloudStorageAccount account,
            string containerName,
            string prefix,
            string delimiter,
            int maxResults,
            BlobContinuationToken currentToken)
        {
            Console.WriteLine($"{prefix,PrefixPad} | {delimiter,DelimiterPad} | DELIMITED LIST {maxResults}");

            var segment = await GetDelimitedSegmentInternalAsync(
                account,
                containerName,
                prefix,
                delimiter,
                maxResults,
                currentToken);

            DumpSegment(prefix, delimiter, segment);

            return segment;
        }

        private static async Task<BlobResultSegment> GetDelimitedSegmentInternalAsync(
            CloudStorageAccount account,
            string containerName,
            string prefix,
            string delimiter,
            int maxResults,
            BlobContinuationToken currentToken)
        {
            var client = account.CreateCloudBlobClient();
            client.DefaultDelimiter = delimiter;
            var container = client.GetContainerReference(containerName);

            return await container.ListBlobsSegmentedAsync(
                prefix: prefix,
                useFlatBlobListing: false,
                blobListingDetails: BlobListingDetails.None,
                maxResults: maxResults,
                currentToken: currentToken,
                options: null,
                operationContext: null);
        }

        private static async Task<BlobResultSegment> GetFlatSegmentAsync(
            CloudBlobContainer container,
            string prefix,
            string delimiter,
            int maxResults)
        {
            Console.WriteLine($"{prefix,PrefixPad} | {delimiter,DelimiterPad} | FLAT LIST {maxResults}");

            var segment = await container.ListBlobsSegmentedAsync(
                prefix,
                useFlatBlobListing: true,
                blobListingDetails: BlobListingDetails.None,
                maxResults: maxResults,
                currentToken: null,
                options: null,
                operationContext: null);

            DumpSegment(prefix, delimiter, segment);

            return segment;
        }

        private static void DumpSegment(
            string prefix,
            string delimiter,
            BlobResultSegment segment)
        {
            foreach (var result in segment.Results)
            {
                if (result is ICloudBlob blob)
                {
                    Console.WriteLine($"{prefix,PrefixPad} | {delimiter,DelimiterPad} | GOT BLOB | {blob.Name.Substring(prefix.Length)}");
                }
                else
                {
                    var directory = (CloudBlobDirectory)result;
                    Console.WriteLine($"{prefix,PrefixPad} | {delimiter,DelimiterPad} | GOT DIR | {directory.Prefix.Substring(prefix.Length)}");
                }
            }
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

        private static string GetNthCharacter(string input, int index)
        {
            for (var i = 0; i < input.Length; i++)
            {
                if (char.IsSurrogate(input[i]))
                {
                    throw new NotSupportedException();
                }
            }

            return input.Substring(index, 1);
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
