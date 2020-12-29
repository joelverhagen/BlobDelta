using Microsoft.Azure.Search.Models;
using System;

namespace Knapcode.SearchDelta
{
    public class DocumentContext
    {
        public DocumentContext(string key, Document document, string filter, int pageIndex, int documentIndex)
        {
            Key = key ?? throw new ArgumentNullException(key);
            Document = document ?? throw new ArgumentNullException(nameof(document));
            Filter = filter;
            PageIndex = pageIndex;
            DocumentIndex = documentIndex;
        }

        public string Key { get; }
        public Document Document { get; }
        public string Filter { get; }
        public int PageIndex { get; }
        public int DocumentIndex { get; }
    }
}
