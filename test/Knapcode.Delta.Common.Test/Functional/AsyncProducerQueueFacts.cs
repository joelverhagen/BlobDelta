using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knapcode.Delta.Common.Test.Support;
using Xunit;

namespace Knapcode.Delta.Common.Test.Functional
{
    public class AsyncProducerQueueFacts
    {
        [Theory]
        [InlineData(3, 5, 1, 0, 0)]
        [InlineData(3, 5, 2, 0, 0)]
        [InlineData(3, 5, 32, 0, 0)]
        [InlineData(3, 5, 2, 0, 1)]
        [InlineData(3, 5, 4, 1, 1)]
        [InlineData(3, 5, 8, 0, 5)]
        [InlineData(3, 5, 16, 0, 10)]
        [InlineData(3, 5, 32, 10, 10)]
        [InlineData(3, 5, 64, 0, 50)]
        [InlineData(3, 5, 2, 0, 25)]
        [InlineData(3, 5, 64, 0, 100)]
        [InlineData(5, 5, 64, 0, 3)]
        public async Task LoadTest(int maxLength, int characterCount, int workerCount, int minSleepMs, int maxSleepMs)
        {
            if (characterCount < 1 || characterCount > 10)
            {
                throw new ArgumentOutOfRangeException("The character count must be between 1 and 10, inclusive.");
            }

            var characters = Enumerable.Range(0, characterCount).Select(c => c.ToString()[0]).ToList();
            var output = new ConcurrentBag<string>();

            using (var queue = new AsyncProducerQueue<string>(
                item => ProduceItemsAsync(minSleepMs, maxSleepMs, item, maxLength, characters, output),
                new[] { string.Empty }))
            {
                var tasks = Enumerable
                    .Range(0, workerCount)
                    .Select(_ => queue.ExecuteAsync())
                    .ToList();

                await Task.WhenAll(tasks);

                for (var length = 0; length <= maxLength; length++)
                {
                    Assert.Equal(Math.Pow(characters.Count, length), output.Count(x => x.Length == length));
                }
            }
        }

        private static async Task<IEnumerable<string>> ProduceItemsAsync(
            int minSleepMs,
            int maxSleepMs,
            string item,
            int maxLength,
            IReadOnlyList<char> characters,
            ConcurrentBag<string> output)
        {
            output.Add(item);

            var sleepMs = ThreadLocalRandom.Next(minSleepMs, maxSleepMs);
            if (sleepMs > 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(sleepMs));
            }

            if (item.Length < maxLength)
            {
                return characters.Select(x => item + x);
            }

            return Enumerable.Empty<string>();
        }
    }
}
