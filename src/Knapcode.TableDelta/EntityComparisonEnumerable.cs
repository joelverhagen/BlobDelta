using Knapcode.Delta.Common;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;

namespace Knapcode.TableDelta
{
    public class EntityComparisonEnumerable<T> : ComparisonEnumerable<EntityContext<T>, EntityContext<T>, EntityComparison<T>> where T : ITableEntity
    {
        public EntityComparisonEnumerable(
            IAsyncEnumerable<EntityContext<T>> left,
            IAsyncEnumerable<EntityContext<T>> right) : base(left, right)
        {
        }

        protected override EntityComparison<T> Compare(EntityContext<T> left, EntityContext<T> right)
        {
            if (left == null)
            {
                if (right == null)
                {
                    throw new InvalidOperationException("Only one side should be null.");
                }

                return NewComparison(EntityComparisonType.MissingFromLeft, left, right);
            }

            if (right == null)
            {
                return NewComparison(EntityComparisonType.MissingFromRight, left, right);
            }

            var partitionKeyComparison = string.CompareOrdinal(left.RawEntity.PartitionKey, right.RawEntity.PartitionKey);
            if (partitionKeyComparison < 0)
            {
                return NewComparison(EntityComparisonType.MissingFromRight, left, right);
            }
            else if (partitionKeyComparison > 0)
            {
                return NewComparison(EntityComparisonType.MissingFromLeft, left, right);
            }

            var rowKeyComparison = string.CompareOrdinal(left.RawEntity.RowKey, right.RawEntity.RowKey);
            if (rowKeyComparison < 0)
            {
                return NewComparison(EntityComparisonType.MissingFromRight, left, right);
            }
            else if (rowKeyComparison > 0)
            {
                return NewComparison(EntityComparisonType.MissingFromLeft, left, right);
            }

            var leftKeys = new HashSet<string>(left.RawEntity.Properties.Keys);
            var rightKeys = new HashSet<string>(right.RawEntity.Properties.Keys);
            if (!leftKeys.SetEquals(rightKeys))
            {
                return NewComparison(EntityComparisonType.DisjointProperties, left, right);
            }

            foreach (var key in leftKeys)
            {
                if (!left.RawEntity.Properties[key].Equals(right.RawEntity.Properties[key]))
                {
                    return NewComparison(EntityComparisonType.DifferentPropertiesValues, left, right);
                }
            }

            return NewComparison(EntityComparisonType.Same, left, right);
        }

        private static EntityComparison<T> NewComparison(EntityComparisonType type, EntityContext<T> left, EntityContext<T> right)
        {
            if (type == EntityComparisonType.MissingFromLeft)
            {
                left = null;
            }

            if (type == EntityComparisonType.MissingFromRight)
            {
                right = null;
            }

            return new EntityComparison<T>(type, left, right);
        }
    }
}
