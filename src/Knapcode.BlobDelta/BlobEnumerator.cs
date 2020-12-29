using Knapcode.Delta.Common;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Knapcode.BlobDelta
{
    public class BlobEnumerable : IAsyncEnumerable<BlobContext>
    {
        private const int MaxPageSize = 5000;

        private readonly CloudBlobContainer _container;
        private readonly BlobContinuationToken _initialToken;
        private readonly string _minBlobName;
        private readonly string _maxBlobName;
        private readonly string _prefix;
        private readonly int _pageSize;

        /// <summary>
        /// Initializes an enumerable that asynchronously enumerates over a blob storage container.
        /// </summary>
        /// <param name="container">The blob storage container to enumerate over.</param>
        public BlobEnumerable(CloudBlobContainer container) : this(
            container,
            initialToken: null,
            prefix: null)
        {
        }

        /// <summary>
        /// Initializes an enumerable that asynchronously enumerates over a blob storage container.
        /// </summary>
        /// <param name="container">The blob storage container to enumerate over.</param>
        /// <param name="initialToken">The initial, inclusive continuation token to use. Can be null.</param>
        /// <param name="prefix">The prefix to limit the enumerated blobs to.</param>
        /// The page size to use. Must be greater or equal to 0 and less than or equal to 5000.
        /// </param>
        public BlobEnumerable(
            CloudBlobContainer container,
            BlobContinuationToken initialToken,
            string prefix) : this(
                container,
                initialToken,
                prefix,
                minBlobName: null,
                maxBlobName: null,
                pageSize: MaxPageSize)
        {
        }

        /// <summary>
        /// Initializes an enumerable that asynchronously enumerates over a blob storage container.
        /// </summary>
        /// <param name="container">The blob storage container to enumerate over.</param>
        /// <param name="initialToken">The initial, inclusive continuation token to use. Can be null.</param>
        /// <param name="prefix">The prefix to limit the enumerated blobs to.</param>
        /// <param name="minBlobName">The inclusive minimum blob name. Can be null.</param>
        /// <param name="maxBlobName">The exclusive maximum blob name. Can be null.</param>
        /// <param name="pageSize">
        /// The page size to use. Must be greater or equal to 0 and less than or equal to 5000.
        /// </param>
        public BlobEnumerable(
            CloudBlobContainer container,
            BlobContinuationToken initialToken,
            string prefix,
            string minBlobName,
            string maxBlobName,
            int? pageSize)
        {
            var actualPageSize = pageSize ?? MaxPageSize;
            if (actualPageSize < 1 || actualPageSize > MaxPageSize)
            {
                throw new ArgumentOutOfRangeException(nameof(pageSize), $"The page size must be between 1 and {MaxPageSize}, inclusive.");
            }

            _container = container ?? throw new ArgumentNullException(nameof(container));
            _initialToken = initialToken;
            _prefix = prefix;
            _minBlobName = minBlobName;
            _maxBlobName = maxBlobName;
            _pageSize = actualPageSize;
        }

        public IAsyncEnumerator<BlobContext> GetEnumerator()
        {
            return new BlobEnumerator(
                _container,
                _initialToken,
                _prefix,
                _minBlobName,
                _maxBlobName,
                _pageSize);
        }

        private class BlobEnumerator : IAsyncEnumerator<BlobContext>
        {
            private readonly CloudBlobContainer _container;
            private readonly string _prefix;
            private readonly int _pageSize;
            private BlobResultSegment _currentSegment;
            private BlobContinuationToken _currentToken;
            private readonly string _minBlobName;
            private readonly string _maxBlobName;
            private IEnumerator<IListBlobItem> _currentEnumerator;
            private bool _complete;
            private ICloudBlob _currentBlob;
            private int _currentSegmentIndex = -1;
            private int _currentBlobIndex;

            public BlobEnumerator(
                CloudBlobContainer container,
                BlobContinuationToken initialToken,
                string prefix,
                string minBlobName,
                string maxBlobName,
                int pageSize)
            {
                _container = container;
                _currentToken = initialToken;
                _minBlobName = minBlobName;
                _maxBlobName = maxBlobName;
                _prefix = prefix;
                _pageSize = pageSize;
            }

            public BlobContext Current { get; private set; }

            public async Task<bool> MoveNextAsync()
            {
                // Get the next blob. If the current blob name is less than the min blob name, getting the next blob
                // until we move past the minimum.
                bool hasCurrent;
                do
                {
                    hasCurrent = await MoveNextInternalAsync().ConfigureAwait(false);
                }
                while (hasCurrent && _minBlobName != null && _currentBlob.Name.CompareTo(_minBlobName) < 0);

                return hasCurrent;
            }

            private async Task<bool> MoveNextInternalAsync()
            {
                if (_complete)
                {
                    return false;
                }

                var hasCurrent = false;
                var isDoneWithSegment = false;
                do
                {
                    // If we haven't gotten a segment yet or we're done with the last one, get the next segment.
                    if (_currentSegment == null || isDoneWithSegment)
                    {
                        // If we have gotten a segment, use the continuation token from the last segment that we got.
                        // If we haven't gotten the sgement before, we use the initial value of the current token,
                        // which is either null (start at the beginning of the container) or some arbitrary starting
                        // point.
                        if (_currentSegment != null)
                        {
                            _currentToken = _currentSegment.ContinuationToken;
                        }

                        _currentSegment = await _container.ListBlobsSegmentedAsync(
                            prefix: _prefix,
                            useFlatBlobListing: true,
                            blobListingDetails: BlobListingDetails.None,
                            maxResults: _pageSize,
                            currentToken: _currentToken,
                            options: null,
                            operationContext: null).ConfigureAwait(false);
                        _currentEnumerator = _currentSegment.Results.GetEnumerator();
                        _currentSegmentIndex++;
                        _currentBlobIndex = -1;
                    }

                    hasCurrent = _currentEnumerator.MoveNext();
                    _currentBlobIndex++;
                    isDoneWithSegment = !hasCurrent;

                    // If we're done with the segment and this segment is the last segment, mark the enumerater as
                    // complete.
                    if (isDoneWithSegment && _currentSegment.ContinuationToken == null)
                    {
                        _complete = true;
                        Current = null;
                        return false;
                    }
                }
                while (!hasCurrent);

                _currentBlob = (ICloudBlob)_currentEnumerator.Current;

                // If the current name is greater than or equal to the max blob name, mark the enumerator as complete.
                if (_maxBlobName != null && _currentBlob.Name.CompareTo(_maxBlobName) >= 0)
                {
                    _complete = true;
                    Current = null;
                    return false;
                }

                Current = new BlobContext(
                    _currentBlob,
                    _currentToken,
                    _currentSegmentIndex,
                    _currentBlobIndex);

                return true;
            }
        }
    }
}
