using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knapcode.Delta.Common;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;

namespace Knapcode.SearchDelta
{
    public class DocumentEnumerable : IAsyncEnumerable<DocumentContext>
    {
        private const int MaxPageSize = 1000;
        private readonly ISearchIndexClient _index;

        public DocumentEnumerable(
            ISearchIndexClient index,
            string keyName) : this(
                index,
                keyName,
                minKey: null,
                maxKey: null,
                pageSize: MaxPageSize)
        {
        }

        public DocumentEnumerable(
            ISearchIndexClient index,
            string keyName,
            string minKey,
            string maxKey,
            int pageSize)
        {
            if (pageSize < 1 || pageSize > MaxPageSize)
            {
                throw new ArgumentOutOfRangeException($"The page size must be greater than 0 and less than or equal to {MaxPageSize}.");
            }

            _index = index ?? throw new ArgumentNullException(nameof(index));
            KeyName = keyName ?? throw new ArgumentNullException(nameof(keyName));
            MinKey = minKey;
            MaxKey = maxKey;
            PageSize = pageSize;
        }

        public string KeyName { get; }
        public string MinKey { get; }
        public string MaxKey { get; }
        public int PageSize { get; }

        public static async Task<DocumentEnumerable> CreateAsync(
            ISearchServiceClient client,
            string indexName)
        {
            return await CreateAsync(
                client,
                indexName,
                minKey: null,
                maxKey: null,
                pageSize: MaxPageSize);
        }

        public static async Task<DocumentEnumerable> CreateAsync(
            ISearchServiceClient client,
            string indexName,
            string minKey,
            string maxKey,
            int pageSize)
        {
            if (client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }

            if (indexName == null)
            {
                throw new ArgumentNullException(nameof(indexName));
            }

            var index = await client.Indexes.GetAsync(indexName);
            var keyField = index.Fields.Single(x => x.IsKey);

            if (!keyField.IsSortable)
            {
                throw new InvalidOperationException(
                    $"The key field '{keyField.Name}' of index '{indexName}' must be sortable.");
            }

            if (!keyField.IsFilterable)
            {
                throw new InvalidOperationException(
                    $"The key field '{keyField.Name}' of index '{indexName}' must be filterable.");
            }

            return new DocumentEnumerable(
                client.Indexes.GetClient(indexName),
                keyField.Name,
                minKey,
                maxKey,
                pageSize);
        }

        public IAsyncEnumerator<DocumentContext> GetEnumerator()
        {
            return new DocumentEnumerator(
                _index,
                KeyName,
                MinKey,
                MaxKey,
                PageSize);
        }

        private class DocumentEnumerator : IAsyncEnumerator<DocumentContext>
        {
            private readonly ISearchIndexClient _index;
            private readonly string _keyName;
            private readonly List<string> _orderBy;
            private string _currentFilter;
            private readonly string _minKey;
            private readonly string _maxKey;
            private readonly int _pageSize;
            private bool _complete;
            private DocumentSearchResult _currentPage;
            private IEnumerator<SearchResult> _currentEnumerator;
            private int _currentPageIndex;
            private int _currentDocumentIndex;
            private Document _currentDocument;

            public DocumentEnumerator(
                ISearchIndexClient index,
                string keyName,
                string minKey,
                string maxKey,
                int pageSize)
            {
                _index = index;
                _keyName = keyName;
                _orderBy = new List<string> { $"{keyName} asc" };
                _currentFilter = MakeGreaterThanOrEqualFilterOrNull(minKey);
                _minKey = minKey;
                _maxKey = maxKey;
                _pageSize = pageSize;
            }

            public DocumentContext Current { get; private set; }

            public async Task<bool> MoveNextAsync()
            {
                if (_complete)
                {
                    return false;
                }

                var hasCurrent = false;
                var isDoneWithPage = false;
                do
                {
                    if (_currentPage == null || isDoneWithPage)
                    {
                        if (_currentPage != null)
                        {
                            _currentFilter = MakeGreaterThanFilter(GetKey(_currentPage.Results.Last().Document));
                        }

                        _currentPage = await _index.Documents.SearchAsync(
                            "*",
                            searchParameters: new SearchParameters
                            {
                                OrderBy = _orderBy,
                                Filter = _currentFilter,
                                Skip = 0,
                                Top = _pageSize,
                            });

                        _currentEnumerator = _currentPage.Results.GetEnumerator();
                        _currentPageIndex++;
                        _currentDocumentIndex = -1;
                    }

                    hasCurrent = _currentEnumerator.MoveNext();
                    _currentDocumentIndex++;
                    isDoneWithPage = !hasCurrent;

                    if (isDoneWithPage && _currentPage.Results.Count < _pageSize)
                    {
                        _complete = true;
                        Current = null;
                        return false;
                    }
                }
                while (!hasCurrent);

                _currentDocument = _currentEnumerator.Current.Document;

                if (_maxKey != null && string.CompareOrdinal(GetKey(_currentDocument), _maxKey) >= 0)
                {
                    _complete = true;
                    Current = null;
                    return false;
                }

                Current = new DocumentContext(
                    GetKey(_currentDocument),
                    _currentDocument,
                    _currentFilter,
                    _currentPageIndex,
                    _currentDocumentIndex);

                return true;
            }

            private string GetKey(Document document)
            {
                return (string)document[_keyName];
            }

            private string MakeGreaterThanOrEqualFilterOrNull(string key)
            {
                if (key == null)
                {
                    return null;
                }

                return $"{_keyName} ge '{key}'";
            }

            private string MakeGreaterThanFilter(string key)
            {
                return $"{_keyName} gt '{key}'";
            }
        }
    }
}
