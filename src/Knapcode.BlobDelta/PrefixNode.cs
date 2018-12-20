using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class PrefixNode
    {
        private readonly List<PrefixNode> _children;

        public PrefixNode(PrefixNode parent, string partialPrefix, BlobContinuationToken token)
        {
            Parent = parent;
            PartialPrefix = partialPrefix ?? throw new ArgumentNullException(nameof(partialPrefix));
            Token = token;
            _children = new List<PrefixNode>();

            if (Parent == null)
            {
                Prefix = partialPrefix;
            }
            else
            {
                Prefix = Parent.Prefix + partialPrefix;
            }
        }

        public PrefixNode Parent { get; }
        public string PartialPrefix { get; }
        public BlobContinuationToken Token { get; }
        public string Prefix { get; }
        public IReadOnlyList<PrefixNode> Children => _children;

        public override string ToString()
        {
            return Prefix;
        }

        public PrefixNode GetOrAddChild(string partialPrefix, BlobContinuationToken token)
        {
            foreach (var existingChild in _children)
            {
                if (existingChild.PartialPrefix == partialPrefix)
                {
                    return existingChild;
                }

                // When the partial prefix is an empty string, this indicates that there is a blob with a name exactly
                // matching the prefix. This is allowed by this data structure. For all other partial prefixes, perform
                // some sanity checks.
                if (existingChild.PartialPrefix != string.Empty)
                {
                    if (existingChild.PartialPrefix.StartsWith(partialPrefix))
                    {
                        throw new ArgumentException("An added child must not have a prefix that is more specific that an existing child.");
                    }

                    if (partialPrefix.StartsWith(existingChild.PartialPrefix))
                    {
                        throw new ArgumentException("An added child must not have a prefix that is less specific that an existing child.");
                    }
                }
            }

            var newChild = new PrefixNode(this, partialPrefix, token);
            _children.Add(newChild);
            return newChild;
        }
    }
}
