using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class BlobContainerEnumerable : IAsyncEnumerable<BlobAndContinuationToken>
    {
        private const int MaxPageSize = 5000;

        private readonly CloudBlobContainer _container;
        private readonly BlobContinuationToken _initialContinuationToken;
        private readonly string _minBlobName;
        private readonly string _maxBlobName;
        private readonly string _prefix;
        private readonly int _pageSize;

        /// <summary>
        /// Initializes an enumerable that asynchronously enumerates over a blob storage container.
        /// </summary>
        /// <param name="container">The blob storage container to enumerate over.</param>
        public BlobContainerEnumerable(CloudBlobContainer container) : this(
            container,
            initialContinuationToken: null,
            minBlobName: null,
            maxBlobName: null,
            prefix: null,
            pageSize: MaxPageSize)
        {
        }

        /// <summary>
        /// Initializes an enumerable that asynchronously enumerates over a blob storage container.
        /// </summary>
        /// <param name="container">The blob storage container to enumerate over.</param>
        /// <param name="initialContinuationToken">The initial, inclusive continuation token to use. Can be null.</param>
        /// <param name="minBlobName">The inclusive minimum blob name. Can be null.</param>
        /// <param name="maxBlobName">The exclusive maximum blob name. Can be null.</param>
        /// <param name="prefix">The prefix to limit the enumerated blobs to.</param>
        /// <param name="pageSize">
        /// The page size to use. Must be greater or equal to 0 and less than or equal to 5000.
        /// </param>
        public BlobContainerEnumerable(
            CloudBlobContainer container,
            BlobContinuationToken initialContinuationToken,
            string minBlobName,
            string maxBlobName,
            string prefix,
            int? pageSize)
        {
            var actualPageSize = pageSize ?? MaxPageSize;
            if (actualPageSize < 1 || actualPageSize > MaxPageSize)
            {
                throw new ArgumentOutOfRangeException(nameof(pageSize), "The page size must be between 1 and 5000, inclusive.");
            }

            _container = container ?? throw new ArgumentNullException(nameof(container));
            _initialContinuationToken = initialContinuationToken;
            _minBlobName = minBlobName;
            _maxBlobName = maxBlobName;
            _prefix = prefix;
            _pageSize = actualPageSize;
        }

        public IAsyncEnumerator<BlobAndContinuationToken> GetEnumerator()
        {
            return new BlobContainerEnumerator(
                _container,
                _initialContinuationToken,
                _minBlobName,
                _maxBlobName,
                _prefix,
                _pageSize);
        }

        private class BlobContainerEnumerator : IAsyncEnumerator<BlobAndContinuationToken>
        {
            private readonly CloudBlobContainer _container;
            private readonly string _prefix;
            private readonly int _pageSize;
            private BlobResultSegment _currentSegment;
            private BlobContinuationToken _currentContinuationToken;
            private readonly string _minBlobName;
            private readonly string _maxBlobName;
            private IEnumerator<IListBlobItem> _currentEnumerator;
            private bool _complete;
            private ICloudBlob _currentBlob;

            public BlobContainerEnumerator(
                CloudBlobContainer container,
                BlobContinuationToken initialContinuationToken,
                string minBlobName,
                string maxBlobName,
                string prefix,
                int pageSize)
            {
                _container = container;
                _currentContinuationToken = initialContinuationToken;
                _minBlobName = minBlobName;
                _maxBlobName = maxBlobName;
                _prefix = prefix;
                _pageSize = pageSize;
            }

            public BlobAndContinuationToken Current
            {
                get
                {
                    if (_currentBlob == null)
                    {
                        return null;
                    }

                    return new BlobAndContinuationToken(_currentBlob, _currentContinuationToken);
                }
            }

            public async Task<bool> MoveNextAsync()
            {
                // Get the next blob. If the current blob name is less than the min blob name, getting the next blob
                // until we move past the minimum.
                bool hasCurrent;
                do
                {
                    hasCurrent = await MoveNextInternalAsync();
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
                            _currentContinuationToken = _currentSegment.ContinuationToken;
                        }

                        _currentSegment = await _container.ListBlobsSegmentedAsync(
                            prefix: _prefix,
                            useFlatBlobListing: true,
                            blobListingDetails: BlobListingDetails.None,
                            maxResults: _pageSize,
                            currentToken: _currentContinuationToken,
                            options: null,
                            operationContext: null);
                        _currentEnumerator = _currentSegment.Results.GetEnumerator();
                    }

                    hasCurrent = _currentEnumerator.MoveNext();
                    isDoneWithSegment = !hasCurrent;

                    // If we're done with the segment and this segment is the last segment, mark the enumerater as
                    // complete.
                    if (isDoneWithSegment && _currentSegment.ContinuationToken == null)
                    {
                        _complete = true;
                        return false;
                    }
                }
                while (!hasCurrent);

                _currentBlob = (ICloudBlob)_currentEnumerator.Current;

                // If the current name is greater than or equal to the max blob name, mark the enumerator as complete.
                if (_maxBlobName != null && _currentBlob.Name.CompareTo(_maxBlobName) >= 0)
                {
                    _complete = true;
                    return false;
                }

                return true;
            }
        }
    }
}
