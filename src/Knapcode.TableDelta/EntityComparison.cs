using Knapcode.Delta.Common;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Knapcode.TableDelta
{
    public class EntityComparison<T> : IComparison where T : ITableEntity
    {
        private static ISet<EntityComparisonType> ValidTypes = new HashSet<EntityComparisonType>(Enum
            .GetValues(typeof(EntityComparisonType))
            .Cast<EntityComparisonType>());

        public EntityComparison(
            EntityComparisonType type,
            EntityContext<T> left,
            EntityContext<T> right)
        {
            if (!ValidTypes.Contains(type))
            {
                throw new ArgumentException("The provided entity comparison type is not valid.", nameof(type));
            }

            if (left == null && right == null)
            {
                throw new ArgumentException($"Either {nameof(left)} or {nameof(right)} (or both) must be non-null.");
            }

            Type = type;
            Left = left;
            Right = right;
        }

        public EntityComparisonType Type { get; }
        public EntityContext<T> Left { get; }
        public EntityContext<T> Right { get; }
        public bool IsMissingFromLeft => Type == EntityComparisonType.MissingFromLeft;
        public bool IsMissingFromRight => Type == EntityComparisonType.MissingFromRight;
    }
}
