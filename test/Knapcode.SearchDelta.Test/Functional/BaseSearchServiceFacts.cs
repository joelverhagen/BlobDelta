using System;
using System.Net;
using Xunit.Abstractions;

namespace Knapcode.SearchDelta.Test.Functional
{
    public abstract class BaseSearchServiceFacts
    {
        static BaseSearchServiceFacts()
        {
            ServicePointManager.DefaultConnectionLimit = 64;
        }

        public ITestOutputHelper Output { get; }

        public BaseSearchServiceFacts(ITestOutputHelper output)
        {
            Output = output;
        }

        private static string GetApiKey()
        {
            const string variableName = "SEARCHDELTA_API_KEY";
            var apiKey = Environment.GetEnvironmentVariable(variableName);

            if (string.IsNullOrWhiteSpace(apiKey))
            {
                throw new InvalidOperationException($"The environment variable '{variableName}' is required.");
            }

            return apiKey.Trim();
        }
    }
}
