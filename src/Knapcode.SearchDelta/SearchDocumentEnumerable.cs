using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knapcode.Delta.Common;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;

namespace Knapcode.SearchDelta
{
    public class SearchDocumentEnumerable : IAsyncEnumerable<DocumentContext>
    {
        private const int MaxPageSize = 1000;

        private readonly ISearchIndexClient _index;
        private readonly string _keyName;
        private readonly string _minKey;
        private readonly string _maxKey;
        private readonly int _pageSize;

        public SearchDocumentEnumerable(
            ISearchIndexClient index,
            string keyName) : this(
                index,
                keyName,
                minKey: null,
                maxKey: null,
                pageSize: MaxPageSize)
        {
        }

        public SearchDocumentEnumerable(
            ISearchIndexClient index,
            string keyName,
            string minKey,
            string maxKey,
            int pageSize)
        {
            _index = index;
            _keyName = keyName;
            _minKey = minKey;
            _maxKey = maxKey;
            _pageSize = pageSize;
        }

        public static async Task<SearchDocumentEnumerable> CreateAsync(
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

        public static async Task<SearchDocumentEnumerable> CreateAsync(
            ISearchServiceClient client,
            string indexName,
            string minKey,
            string maxKey,
            int pageSize)
        {
            var index = await client.Indexes.GetAsync(indexName);
            var keyField = index.Fields.Single(x => x.IsKey);

            if (!keyField.IsSortable)
            {
                throw new ArgumentException(
                    $"The key field '{keyField.Name}' of index '{indexName}' must be sortable.",
                    nameof(indexName));
            }

            if (!keyField.IsFilterable)
            {
                throw new ArgumentException(
                    $"The key field '{keyField.Name}' of index '{indexName}' must be filterable.",
                    nameof(indexName));
            }

            return new SearchDocumentEnumerable(
                client.Indexes.GetClient(indexName),
                keyField.Name,
                minKey,
                maxKey,
                pageSize);
        }

        public IAsyncEnumerator<DocumentContext> GetEnumerator()
        {
            return new SearchDocumentEnumerator(
                _index,
                _keyName,
                _minKey,
                _maxKey,
                _pageSize);
        }

        private class SearchDocumentEnumerator : IAsyncEnumerator<DocumentContext>
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

            public SearchDocumentEnumerator(
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
