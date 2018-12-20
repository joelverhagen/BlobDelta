using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class PrefixTreeBuilder
    {
        private readonly ILogger<PrefixTreeBuilder> _logger;

        public PrefixTreeBuilder(ILogger<PrefixTreeBuilder> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<PrefixNode> EnumerateLeadingCharacters(
            CloudStorageAccount account,
            string containerName,
            PrefixNode parent)
        {
            using (_logger.BeginScope(
                "Enumerating leading characters for prefix {Prefix} in container {ContainerName} on account {AccountUrl}.",
                parent.Prefix,
                containerName,
                account.BlobEndpoint.AbsoluteUri))
            {
                await PopulateNodeWithLeadingCharacters(account, containerName, parent);
                return parent;
            }
        }

        public async Task<PrefixNode> EnumerateLeadingCharacters(
            CloudStorageAccount account,
            string containerName,
            string prefix)
        {
            var node = new PrefixNode(parent: null, partialPrefix: prefix, token: null);
            return await EnumerateLeadingCharacters(account, containerName, node);
        }

        private async Task PopulateNodeWithLeadingCharacters(
            CloudStorageAccount account,
            string containerName,
            PrefixNode node)
        {
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            var initialNode = await GetInitialNodeOrNullAsync(account, container, node);
            if (initialNode == null)
            {
                return;
            }

            var currentToken = initialNode.Token;
            var delimiter = initialNode.PartialPrefix;

            var resultCount = 2;
            while (resultCount > 1)
            {
                // Collapse all blob names starting with the current delimiter into one record. For example, suppose
                // the following blobs are available:
                //
                //   Name | Type
                //   ---- | ----
                //   a1   | blob
                //   a2   | blob
                //   b1   | blob
                //   b2   | blob
                //
                // If the delimiter "a" is provided and max results is 2, the response will be:
                //
                //   Name | Type
                //   ---- | ---------
                //   a    | directory
                //   b1   | blob
                //
                // In this case, the next blob after the directory does not have the delimiter. That is why the second
                // record is a blob. Now suppose the following blobs are available:
                //
                //   Name | Type
                //   ---- | ----
                //   a1   | blob
                //   a2   | blob
                //   ba1  | blob
                //   ba2  | blob
                //   bb   | blob
                //
                // In this case, the response will be:
                //
                //   Name | Type
                //   ---- | ---------
                //   a    | directory
                //   ba   | directory
                //
                // The blob "ba1" has a different first character but the second character is the delimiter so
                // it becomes a directory as well. This doesn't matter, since all we care about is the "b" part.
                var segment = await GetDelimitedSegmentAsync(
                    account,
                    containerName,
                    node.Prefix,
                    delimiter,
                    currentToken,
                    maxResults: 2);
                var results = segment.Results.ToList();
                resultCount = results.Count;

                if (resultCount > 1)
                {
                    // Fetch a token whic allows us to step over the current delimiter.
                    currentToken = await GetDelimitedContinuationTokenAsync(
                        account,
                        containerName,
                        node.Prefix,
                        delimiter,
                        currentToken);

                    // Determine the next delimiter by getting the first character of the next blob name, following the
                    // prefix. The next result might be a blob, but it also might be directory.
                    var second = results[1];
                    if (second is ICloudBlob nextBlob)
                    {
                        delimiter = GetNthCharacter(nextBlob.Name, node.Prefix.Length);
                    }
                    else if (second is CloudBlobDirectory nextDirectory)
                    {
                        delimiter = GetNthCharacter(nextDirectory.Prefix, node.Prefix.Length);
                    }
                    else
                    {
                        throw new NotSupportedException($"The blob type {second.GetType().FullName} is not supported.");
                    }

                    _logger.LogInformation("Now using delimiter {Delimiter}", delimiter);
                    node.GetOrAddChild(delimiter, currentToken);
                }
            }

            _logger.LogInformation("Done. Found {Count} leading characters.", node.Children.Count);
        }

        private async Task<PrefixNode> GetInitialNodeOrNullAsync(
            CloudStorageAccount account,
            CloudBlobContainer container,
            PrefixNode node)
        {
            _logger.LogInformation("Fetching first blob name with prefix {Prefix}.", node.Prefix);

            // We fetch two results here because it's possible that there is a blob with a name exactly matching the
            // prefix. This is a special case where a node with an empty string prefix is added.
            var segment = await container.ListBlobsSegmentedAsync(
                node.Prefix,
                useFlatBlobListing: true,
                blobListingDetails: BlobListingDetails.None,
                maxResults: 2,
                currentToken: null,
                options: null,
                operationContext: null);

            LogSegment(node.Prefix, delimiter: null, segment: segment);

            var results = segment.Results.Cast<ICloudBlob>().ToList();
            if (results.Count == 0)
            {
                _logger.LogInformation("Done. There are no blobs with prefix {Prefix}.", node.Prefix);
                return null;
            }

            string firstBlobName;
            BlobContinuationToken token;
            if (results[0].Name.Length == node.Prefix.Length)
            {
                _logger.LogInformation("There is a blob name matching the prefix {Prefix} exactly.", node.Prefix);

                if (results.Count > 1)
                {
                    firstBlobName = results[1].Name;

                    // Get a continuation token skipping that initial blob.
                    var segmentWithBlobMatchingSegment = await container.ListBlobsSegmentedAsync(
                        node.Prefix,
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.None,
                        maxResults: 1,
                        currentToken: null,
                        options: null,
                        operationContext: null);
                    token = segmentWithBlobMatchingSegment.ContinuationToken;
                }
                else
                {
                    _logger.LogInformation("Done. There is only one blob and its name exactly matching the prefix {Prefix}.", node.Prefix);
                    return null;
                }
            }
            else
            {
                firstBlobName = results[0].Name;
                token = null;
            }

            var delimiter = GetNthCharacter(firstBlobName, node.Prefix.Length);
            _logger.LogInformation("Starting with delimiter {Delimiter}", delimiter);
            return node.GetOrAddChild(delimiter, token);
        }

        private async Task<BlobResultSegment> GetDelimitedSegmentAsync(
            CloudStorageAccount account,
            string containerName,
            string prefix,
            string delimiter,
            BlobContinuationToken currentToken,
            int maxResults)
        {
            _logger.LogInformation("Fetching a segment with prefix {Prefix}, delimiter {Delimiter}.", prefix, delimiter);

            var segment = await GetDelimitedSegmentInternalAsync(
                account,
                containerName,
                prefix,
                delimiter,
                maxResults,
                currentToken);

            LogSegment(prefix, delimiter, segment);

            return segment;
        }

        private async Task<BlobContinuationToken> GetDelimitedContinuationTokenAsync(
            CloudStorageAccount account,
            string containerName,
            string prefix,
            string delimiter,
            BlobContinuationToken currentToken)
        {
            _logger.LogInformation("Fetching a continuation token with {Prefix}, delimiter {Delimiter}.", prefix, delimiter);

            var segment = await GetDelimitedSegmentInternalAsync(
                account,
                containerName,
                prefix,
                delimiter,
                maxResults: 1,
                currentToken: currentToken);

            LogSegment(prefix, delimiter, segment);

            return segment.ContinuationToken;
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

        private static string GetNthCharacter(string input, int index)
        {
            if (char.IsSurrogatePair(input, index))
            {
                return input.Substring(index, 2);
            }
            else
            {
                return input.Substring(index, 1);
            }
        }

        private void LogSegment(
            string prefix,
            string delimiter,
            BlobResultSegment segment)
        {
            var blobs = new List<string>();
            foreach (var result in segment.Results)
            {
                if (result is ICloudBlob blob)
                {
                    blobs.Add($"{blob.Name} (blob)");
                }
                else if (result is CloudBlobDirectory directory)
                {
                    blobs.Add($"{directory.Prefix} (directory)");
                }
                else
                {
                    blobs.Add($"{result.Uri.AbsoluteUri} ({result.GetType().FullName})");
                }
            }

            _logger.LogInformation("Got a segment with {Count} results. Results: {Blobs}", blobs.Count, blobs);
        }

        /// <summary>
        /// Note: this is currently unused. It was written in an attempted, more clever, prefix tree builder.
        /// </summary>
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
                    return other.Substring(startIndex, (i - startIndex) + 1);
                }
            }

            return null;
        }

        /// <summary>
        /// Note: this is currently unused. It was written in an attempted, more clever, prefix tree builder.
        /// </summary>
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
    }
}
