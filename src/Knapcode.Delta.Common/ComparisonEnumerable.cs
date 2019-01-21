using System;
using System.Threading.Tasks;

namespace Knapcode.Delta.Common
{
    public delegate TComparison Compare<TLeft, TRight, TComparison>(TLeft left, TRight right);

    public abstract class ComparisonEnumerable<TLeft, TRight, TComparison> : IAsyncEnumerable<TComparison>
        where TLeft : class
        where TRight : class
        where TComparison : IComparison
    {
        private readonly IAsyncEnumerable<TLeft> _left;
        private readonly IAsyncEnumerable<TRight> _right;

        public ComparisonEnumerable(
            IAsyncEnumerable<TLeft> left,
            IAsyncEnumerable<TRight> right)
        {
            _left = left ?? throw new ArgumentNullException(nameof(left));
            _right = right ?? throw new ArgumentNullException(nameof(right));
        }

        protected abstract TComparison Compare(TLeft left, TRight right);

        public IAsyncEnumerator<TComparison> GetEnumerator()
        {
            return new ComparisonEnumerator(
                _left.GetEnumerator(),
                _right.GetEnumerator(),
                Compare);
        }

        private class ComparisonEnumerator : IAsyncEnumerator<TComparison>
        {
            private readonly IAsyncEnumerator<TLeft> _left;
            private readonly IAsyncEnumerator<TRight> _right;
            private readonly Compare<TLeft, TRight, TComparison> _compare;
            private bool _hasFirst;
            private bool _completedLeft;
            private bool _completedRight;

            public ComparisonEnumerator(
                IAsyncEnumerator<TLeft> left,
                IAsyncEnumerator<TRight> right,
                Compare<TLeft, TRight, TComparison> compare)
            {
                _left = left ?? throw new ArgumentNullException(nameof(left));
                _right = right ?? throw new ArgumentNullException(nameof(right));
                _compare = compare ?? throw new ArgumentNullException(nameof(compare));
                _hasFirst = false;
                _completedLeft = false;
                _completedRight = false;
            }

            public TComparison Current { get; private set; }

            public async Task<bool> MoveNextAsync()
            {
                if (!_hasFirst)
                {
                    await MoveBothNextAsync().ConfigureAwait(false);
                    _hasFirst = true;
                }

                if (_completedLeft && _completedRight)
                {
                    return false;
                }

                var left = _completedLeft ? null : _left.Current;
                var right = _completedRight ? null : _right.Current;

                var comparison = _compare(left, right);
                if (comparison.IsMissingFromLeft)
                {
                    await MoveRightNextAsync().ConfigureAwait(false);
                }
                else if (comparison.IsMissingFromRight)
                {
                    await MoveLeftNextAsync().ConfigureAwait(false);
                }
                else
                {
                    await MoveBothNextAsync().ConfigureAwait(false);
                }

                Current = comparison;
                return true;
            }

            private async Task MoveBothNextAsync()
            {
                var moveLeftNext = MoveLeftNextAsync();
                var moveRightNext = MoveRightNextAsync();
                await moveLeftNext.ConfigureAwait(false);
                await moveRightNext.ConfigureAwait(false);
            }

            private async Task MoveLeftNextAsync()
            {
                await Task.Yield();
                if (!await _left.MoveNextAsync().ConfigureAwait(false))
                {
                    _completedLeft = true;
                }
            }

            private async Task MoveRightNextAsync()
            {
                await Task.Yield();
                if (!await _right.MoveNextAsync().ConfigureAwait(false))
                {
                    _completedRight = true;
                }
            }
        }
    }
}
