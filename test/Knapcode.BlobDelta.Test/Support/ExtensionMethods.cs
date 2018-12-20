using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Knapcode.BlobDelta.Test.Support
{
    public static class ExtensionMethods
    {
        public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> enumerable)
        {
            var enumerator = enumerable.GetEnumerator();
            return await enumerator.ToListAsync();
        }

        public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerator<T> enumerator)
        {
            var list = new List<T>();
            while (await enumerator.MoveNextAsync())
            {
                list.Add(enumerator.Current);
            }

            return list;
        }

        public static async Task SkipAsync<T>(this IAsyncEnumerator<T> enumerator, int count)
        {
            var skipped = 0;
            while (skipped < count && await enumerator.MoveNextAsync())
            {
                skipped++;
            }
        }

        public static string[] GetBlobNames(this IEnumerable<BlobContext> output)
        {
            return output.Select(x => x.Blob.Name).ToArray();
        }
    }
}
