using System;
using System.Collections.Generic;

namespace Knapcode.BlobDelta.Sample
{
    public class Node
    {
        private readonly List<Node> _children;

        public Node(Node parent, string partialPrefix)
        {
            Parent = parent;
            PartialPrefix = partialPrefix ?? throw new ArgumentNullException(nameof(partialPrefix));
            _children = new List<Node>();

            if (Parent == null)
            {
                Prefix = partialPrefix;
            }
            else
            {
                Prefix = Parent.Prefix + partialPrefix;
            }
        }

        public Node Parent { get; }
        public string PartialPrefix { get; }
        public string Prefix { get; }
        public IReadOnlyList<Node> Children => _children;

        public override string ToString()
        {
            return Prefix;
        }

        public Node GetOrAddChild(string partialPrefix)
        {
            foreach (var existingChild in _children)
            {
                if (existingChild.PartialPrefix == partialPrefix)
                {
                    return existingChild;
                }

                if (existingChild.PartialPrefix.StartsWith(partialPrefix))
                {
                    throw new ArgumentException("An added child must not have a prefix that is more specific that an existing child.");
                }

                if (partialPrefix.StartsWith(existingChild.PartialPrefix))
                {
                    throw new ArgumentException("An added child must not have a prefix that is less specific that an existing child.");
                }
            }

            var newChild = new Node(this, partialPrefix);
            _children.Add(newChild);
            return newChild;
        }
    }
}
