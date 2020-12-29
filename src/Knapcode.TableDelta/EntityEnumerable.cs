using Knapcode.Delta.Common;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Knapcode.TableDelta
{
    public class EntityEnumerable<T> : IAsyncEnumerable<EntityContext<T>> where T : ITableEntity
    {
        private const int MaxTakeCount = 1000;

        private readonly CloudTable _table;
        private readonly int _takeCount;

        public EntityEnumerable(CloudTable table, int? takeCount)
        {
            var actualTakeCount = takeCount ?? MaxTakeCount;
            if (actualTakeCount < 1 || actualTakeCount > MaxTakeCount)
            {
                throw new ArgumentOutOfRangeException(nameof(actualTakeCount), $"The take count must be between 1 and {MaxTakeCount}, inclusive.");
            }

            _table = table;
            _takeCount = actualTakeCount;
        }

        public IAsyncEnumerator<EntityContext<T>> GetEnumerator() => new EntityEnumerator(_table, _takeCount);

        private class EntityEnumerator : IAsyncEnumerator<EntityContext<T>>
        {
            private readonly CloudTable _table;
            private readonly TableQuery<DynamicTableEntity> _query;
            private TableQuerySegment<DynamicTableEntity> _currentSegment;
            private IEnumerator<DynamicTableEntity> _currentEnumerator;
            private TableContinuationToken _currentToken;
            private bool _complete;
            private int _currentSegmentIndex = -1;
            private int _currentEntityIndex;

            public EntityEnumerator(CloudTable table, int takeCount)
            {
                _table = table;
                _query = new TableQuery<DynamicTableEntity>
                {
                    TakeCount = takeCount,
                };
            }

            public EntityContext<T> Current { get; private set; }

            public async Task<bool> MoveNextAsync()
            {
                if (_complete)
                {
                    return false;
                }

                var isDoneWithSegment = false;
                bool hasCurrent;
                do
                {
                    if (_currentSegment == null || isDoneWithSegment)
                    {
                        if (_currentSegment != null)
                        {
                            _currentToken = _currentSegment.ContinuationToken;
                        }

                        _currentSegment = await _table.ExecuteQuerySegmentedAsync(_query, _currentToken).ConfigureAwait(false);
                        _currentEnumerator = _currentSegment.Results.GetEnumerator();
                        _currentSegmentIndex++;
                        _currentEntityIndex = -1;
                    }

                    hasCurrent = _currentEnumerator.MoveNext();
                    _currentEntityIndex++;
                    isDoneWithSegment = !hasCurrent;

                    if (isDoneWithSegment && _currentSegment.ContinuationToken == null)
                    {
                        _complete = true;
                        Current = default;
                        return false;
                    }
                }
                while (!hasCurrent);

                Current = new EntityContext<T>(
                    _currentEnumerator.Current,
                    _currentToken,
                    _currentSegmentIndex,
                    _currentEntityIndex);

                return true;
            }
        }
    }
}
