using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Index = Microsoft.Azure.Search.Models.Index;

namespace Knapcode.SearchDelta.Test.Functional
{
    public abstract class BaseSearchServiceFacts : IAsyncLifetime
    {
        public ITestOutputHelper Output { get; }
        public string ServiceName { get; }
        public SearchServiceClient ServiceClient { get; }
        public ISearchIndexClient IndexClient { get; }
        public string IndexName { get; }

        public BaseSearchServiceFacts(ITestOutputHelper output)
        {
            Output = output;
            ServiceName = GetServiceName();
            IndexName = GetIndexName();
            output.WriteLine($"Using search service '{ServiceName}' and index '{IndexName}'.");
            ServiceClient = new SearchServiceClient(
                ServiceName,
                new SearchCredentials(GetApiKey()));
            IndexClient = ServiceClient.Indexes.GetClient(IndexName);
        }

        public KeyedDocument[] GetKeyedDocuments(params string[] keys)
        {
            return keys
                .Select(x => new KeyedDocument { Key = x })
                .ToArray();
        }

        public async Task CreateIndexAsync<T>(params T[] documents) where T : class
        {
            await ServiceClient.Indexes.CreateAsync(new Index
            {
                Name = IndexName,
                Fields = FieldBuilder.BuildForType<T>(),
            });

            if (documents.Length > 0)
            {
                await IndexDocumentsAsync(documents);
            }
        }

        private async Task IndexDocumentsAsync<T>(T[] documents) where T : class
        {
            var batch = new IndexBatch<T>(documents.Select(x => IndexAction.Upload(x)));
            await IndexClient.Documents.IndexAsync(batch);

            const int requiredConfirmations = 3;
            var confirmations = 0;

            while (confirmations < requiredConfirmations)
            {
                var results = await IndexClient.Documents.SearchAsync(
                    "*",
                    new SearchParameters { Skip = 0, Top = documents.Length });

                if (results.Results.Count >= documents.Length)
                {
                    Output.WriteLine(
                       $"There are {confirmations} confirmations of the completed indexing. " +
                       $"There should be {requiredConfirmations}. " +
                       $"Waiting 500ms.");
                    confirmations++;
                }
                else
                {
                    Output.WriteLine(
                       $"There are {results.Results.Count} documents in the index. " +
                       $"There should be {documents.Length}. " +
                       $"Waiting 500ms.");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500));
            }
        }

        private static string GetServiceName()
        {
            return GetEnvironmentVariable("SEARCHDELTA_SERVICE_NAME");
        }

        private static string GetApiKey()
        {
            return GetEnvironmentVariable("SEARCHDELTA_API_KEY");
        }

        private static string GetIndexName()
        {
            return GetEnvironmentVariable("SEARCHDELTA_INDEX_NAME");
        }

        private static string GetEnvironmentVariable(string variableName)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (string.IsNullOrWhiteSpace(value))
            {
                throw new InvalidOperationException($"The environment variable '{variableName}' is required.");
            }

            return value.Trim();
        }

        public async Task InitializeAsync()
        {
            if (await ServiceClient.Indexes.ExistsAsync(IndexName))
            {
                await ServiceClient.Indexes.DeleteAsync(IndexName);
            }
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

        public class KeyedDocument
        {
            [Key]
            [IsSortable]
            [IsFilterable]
            public string Key { get; set; }
        }

        public class CarDocument : KeyedDocument
        {
            public int? NumberOfWheels { get; set; }
            public string Model { get; set; }
            public string Make { get; set; }
        }

        public class GasCarDocument : CarDocument
        {
            public int? MPG { get; set; }
        }

        public class EletricCarDocument : CarDocument
        {
            public int? KWHP100M { get; set; }
        }
    }
}
