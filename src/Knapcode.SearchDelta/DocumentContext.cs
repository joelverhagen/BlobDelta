using Microsoft.Azure.Search.Models;

namespace Knapcode.SearchDelta
{
    public class DocumentContext
    {
        public DocumentContext(Document document, string filter, int pageIndex, int documentIndex)
        {
            Document = document;
            Filter = filter;
            PageIndex = pageIndex;
            DocumentIndex = documentIndex;
        }

        public Document Document { get; }
        public string Filter { get; }
        public int PageIndex { get; }
        public int DocumentIndex { get; }
    }
}
