using Knapcode.Delta.Common;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Knapcode.BlobDelta
{
    public class PrefixTreeBuilder
    {
        private readonly PrefixTreeBuilderConfiguration _configuration;
        private readonly ILogger _logger;

        public PrefixTreeBuilder(
            PrefixTreeBuilderConfiguration configuration,
            ILogger<PrefixTreeBuilder> logger)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _logger = new LoggerWrapper(
                _configuration.MinimumLogLevel,
                logger ?? throw new ArgumentNullException(nameof(logger)));
        }

        public async Task<PrefixNode> EnumerateLeadingCharactersAsync(
            CloudStorageAccount account,
            string containerName,
            string prefix,
            int depth)
        {
            var node = new PrefixNode(parent: null, partialPrefix: prefix, token: null);
            await EnumerateLeadingCharactersAsync(account, containerName, node, depth).ConfigureAwait(false);
            return node;
        }

        public async Task<PrefixNode> EnumerateLeadingCharactersAsync(
            CloudStorageAccount account,
            string containerName,
            PrefixNode parent,
            int depth)
        {
            if (depth > 0)
            {
                using (var queue = new AsyncProducerQueue<PrefixNodeAndDepth>(
                    item => PopulateNodeWithLeadingCharactersAsync(account, containerName, item),
                    new[] { new PrefixNodeAndDepth(parent, depth) }))
                {
                    var workerTasks = Enumerable
                        .Range(0, _configuration.WorkerCount)
                        .Select(_ => queue.ExecuteAsync())
                        .ToList();

                    await Task.WhenAll(workerTasks).ConfigureAwait(false);
                }
            }

            return parent;
        }

        private async Task<IEnumerable<PrefixNodeAndDepth>> PopulateNodeWithLeadingCharactersAsync(
            CloudStorageAccount account,
            string containerName,
            PrefixNodeAndDepth item)
        {
            // Enumerate the node, if necessary.
            if (!item.Node.IsEnumerated)
            {
                using (_logger.BeginScope(
                    "Enumerating leading characters for prefix {Prefix} in container {ContainerName} on account {AccountUrl}.",
                    item.Node.Prefix,
                    containerName,
                    account.BlobEndpoint.AbsoluteUri))
                {
                    await PopulateNodeWithLeadingCharactersAsync(account, containerName, item.Node).ConfigureAwait(false);
                }
            }

            // Enqueue the children, if we haven't hit our depth limit.
            if (item.Depth > 1)
            {
                return item
                    .Node
                    .Children
                    .Select(x => new PrefixNodeAndDepth(x, item.Depth - 1));
            }

            return Enumerable.Empty<PrefixNodeAndDepth>();
        }

        private async Task PopulateNodeWithLeadingCharactersAsync(
            CloudStorageAccount account,
            string containerName,
            PrefixNode node)
        {
            _logger.LogInformation("Starting the enumeration of leading characters with '{Prefix}'.", node.Prefix);

            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            var initialNode = await GetInitialNodeOrNullAsync(account, container, node).ConfigureAwait(false);
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
                    maxResults: 2).ConfigureAwait(false);
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
                        currentToken).ConfigureAwait(false);

                    // Determine the next delimiter by getting the first character of the next blob name, following the
                    // prefix. The next result might be a blob, but it also might be directory.
                    var second = results[1];
                    if (second is ICloudBlob nextBlob)
                    {
                        delimiter = StringUtility.GetNthCharacter(nextBlob.Name, node.Prefix.Length);
                    }
                    else if (second is CloudBlobDirectory nextDirectory)
                    {
                        delimiter = StringUtility.GetNthCharacter(nextDirectory.Prefix, node.Prefix.Length);
                    }
                    else
                    {
                        throw new NotSupportedException($"The blob type {second.GetType().FullName} is not supported.");
                    }

                    _logger.LogDebug("Now using delimiter '{Delimiter}'", delimiter);
                    node.GetOrAddChild(delimiter, currentToken);
                }
            }

            node.MarkAsEnumerated();

            _logger.LogInformation("Done. Found {Count} leading characters for prefix '{Prefix}'.", node.Children.Count, node.Prefix);
        }

        private async Task<PrefixNode> GetInitialNodeOrNullAsync(
            CloudStorageAccount account,
            CloudBlobContainer container,
            PrefixNode node)
        {
            _logger.LogDebug("Fetching first blob name with prefix '{Prefix}'.", node.Prefix);

            // We fetch two results here because it's possible that there is a blob with a name exactly matching the
            // prefix. This is a special case where a node with an empty string prefix is added.
            var segment = await container.ListBlobsSegmentedAsync(
                node.Prefix,
                useFlatBlobListing: true,
                blobListingDetails: BlobListingDetails.None,
                maxResults: 2,
                currentToken: null,
                options: null,
                operationContext: null).ConfigureAwait(false);

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
                _logger.LogDebug("There is a blob name matching the prefix {Prefix} exactly.", node.Prefix);
                node.MarkAsBlob();

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
                        operationContext: null).ConfigureAwait(false);
                    token = segmentWithBlobMatchingSegment.ContinuationToken;
                }
                else
                {
                    _logger.LogInformation("Done. There is only one blob and its name exactly matching the prefix '{Prefix}'.", node.Prefix);
                    node.MarkAsEnumerated();
                    return null;
                }
            }
            else
            {
                firstBlobName = results[0].Name;
                token = null;
            }

            var delimiter = StringUtility.GetNthCharacter(firstBlobName, node.Prefix.Length);
            _logger.LogDebug("Starting with delimiter '{Delimiter}'.", delimiter);
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
            _logger.LogDebug("Fetching a segment with prefix '{Prefix}', delimiter '{Delimiter}'.", prefix, delimiter);

            var segment = await GetDelimitedSegmentInternalAsync(
                account,
                containerName,
                prefix,
                delimiter,
                maxResults,
                currentToken).ConfigureAwait(false);

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
            _logger.LogDebug("Fetching a continuation token with '{Prefix}', delimiter '{Delimiter}'.", prefix, delimiter);

            var segment = await GetDelimitedSegmentInternalAsync(
                account,
                containerName,
                prefix,
                delimiter,
                maxResults: 1,
                currentToken: currentToken).ConfigureAwait(false);

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
                operationContext: null).ConfigureAwait(false);
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

            _logger.LogDebug("Got a segment with {Count} results. Results: {Blobs}", blobs.Count, blobs);
        }

        private class PrefixNodeAndDepth
        {
            public PrefixNodeAndDepth(PrefixNode node, int depth)
            {
                Node = node;
                Depth = depth;
            }

            public PrefixNode Node { get; }
            public int Depth { get; }
        }
    }
}
