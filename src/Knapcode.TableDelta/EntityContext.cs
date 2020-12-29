using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace Knapcode.TableDelta
{
    public class EntityContext<T> where T : ITableEntity
    {
        private readonly Lazy<T> _lazyEntity;

        public EntityContext(DynamicTableEntity rawEntity, TableContinuationToken continuationToken, int segmentIndex, int entityIndex)
        {
            RawEntity = rawEntity;
            _lazyEntity = new Lazy<T>(() =>
            {
                var entity = Activator.CreateInstance<T>();
                entity.PartitionKey = RawEntity.PartitionKey;
                entity.RowKey = RawEntity.RowKey;
                entity.Timestamp = RawEntity.Timestamp;
                entity.ETag = RawEntity.ETag;
                TableEntity.ReadUserObject(entity, RawEntity.Properties, operationContext: null);
                return entity;
            });
            ContinuationToken = continuationToken;
            SegmentIndex = segmentIndex;
            EntityIndex = entityIndex;
        }

        public DynamicTableEntity RawEntity { get; }
        public T Entity => _lazyEntity.Value;
        public TableContinuationToken ContinuationToken { get; }
        public int SegmentIndex { get; }
        public int EntityIndex { get; }
    }
}
