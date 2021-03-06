﻿using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;

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

        public IReadOnlyList<PrefixNode> GetNonEnumeratedNodes()
        {
            var output = new List<PrefixNode>();
            BreadthFirstSearch(output, x => !x.IsEnumerated);
            return output;
        }

        public IReadOnlyList<PrefixNode> GetBlobNodes()
        {
            var output = new List<PrefixNode>();
            BreadthFirstSearch(output, x => x.IsBlob);
            return output;
        }

        private void BreadthFirstSearch(
            List<PrefixNode> output,
            Predicate<PrefixNode> shouldInclude)
        {
            var nodes = new Queue<PrefixNode>();
            nodes.Enqueue(this);

            while (nodes.Count > 0)
            {
                var node = nodes.Dequeue();

                if (shouldInclude(node))
                {
                    output.Add(node);
                }

                foreach (var child in node.Children)
                {
                    nodes.Enqueue(child);
                }
            }
        }

        public PrefixNode Parent { get; }
        public string PartialPrefix { get; }
        public BlobContinuationToken Token { get; }
        public string Prefix { get; }

        public bool IsEnumerated { get; private set; }
        public bool IsBlob { get; private set; }
        public IReadOnlyList<PrefixNode> Children => _children;

        public override string ToString()
        {
            return Prefix;
        }

        public void MarkAsEnumerated()
        {
            IsEnumerated = true;
        }

        public void MarkAsBlob()
        {
            IsBlob = true;
        }

        public PrefixNode GetOrAddChild(string partialPrefix, BlobContinuationToken token)
        {
            foreach (var existingChild in Children)
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

            var newChild = new PrefixNode(this, partialPrefix, token);
            _children.Add(newChild);
            return newChild;
        }
    }
}
