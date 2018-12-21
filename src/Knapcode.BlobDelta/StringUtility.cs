using System;
using System.Collections.Generic;
using System.Linq;

namespace Knapcode.BlobDelta
{
    internal static class StringUtility
    {
        public static string GetNthCharacter(string input, int index)
        {
            if (char.IsSurrogatePair(input, index))
            {
                return input.Substring(index, 2);
            }
            else
            {
                return input.Substring(index, 1);
            }
        }

        /// <summary>
        /// Note: this is currently unused. It was written in an attempted, more clever, prefix tree builder.
        /// </summary>
        public static string GetLastUnsharedPrefix(IReadOnlyList<string> input, int startIndex)
        {
            if (input == null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            if (input.Count == 0)
            {
                throw new ArgumentException("There must be at least one string in the input list.", nameof(input));
            }

            // Verify the list is sorted and unique.
            for (var i = 1; i < input.Count; i++)
            {
                if (string.CompareOrdinal(input[i - 1], input[i]) >= 0)
                {
                    throw new ArgumentException("The list must have unique items and must be sorted ordinally, ascending.", nameof(input));
                }
            }

            var last = input.Last();

            // Verify the characters before the start index are the same and that the start index is valid.
            for (var otherIndex = 0; otherIndex < input.Count - 1; otherIndex++)
            {
                var other = input[otherIndex];
                if (startIndex >= other.Length)
                {
                    throw new ArgumentException("The start index must be valid for all input strings.", nameof(input));
                }

                for (var i = 0; i < startIndex; i++)
                {
                    if (other[i] != last[i])
                    {
                        throw new ArgumentException("All of the characters up to the start index must be the same in all strings.", nameof(input));
                    }
                }
            }

            for (var i = startIndex; i < last.Length; i++)
            {
                var allMatching = true;
                string other = null;
                for (var otherIndex = input.Count - 2; otherIndex >= 0; otherIndex--)
                {
                    other = input[otherIndex];

                    // TODO: handle i being out of bounds in other
                    if (last[i] != other[i])
                    {
                        allMatching = false;
                        break;
                    }
                }

                if (!allMatching)
                {
                    return other.Substring(startIndex, (i - startIndex) + 1);
                }
            }

            return null;
        }

        /// <summary>
        /// Note: this is currently unused. It was written in an attempted, more clever, prefix tree builder.
        /// </summary>
        public static string GetLongestSharedPrefix(IReadOnlyList<string> input, int startIndex)
        {
            if (input == null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            if (input.Count == 0)
            {
                throw new ArgumentException("There must be at least one string in the input list.", nameof(input));
            }

            var first = input[0];
            if (input.Count == 1)
            {
                return first;
            }

            for (var candidateLength = input[0].Length; candidateLength > startIndex; candidateLength--)
            {
                var allMatching = true;
                for (var otherIndex = 1; otherIndex < input.Count; otherIndex++)
                {
                    if (string.CompareOrdinal(first, startIndex, input[otherIndex], startIndex, candidateLength - startIndex) != 0)
                    {
                        allMatching = false;
                        break;
                    }
                }

                if (allMatching)
                {
                    // Don't split surrogate pairs.
                    if (char.IsHighSurrogate(first[candidateLength - 1]))
                    {
                        return first.Substring(startIndex, (candidateLength - startIndex) - 1);
                    }
                    else
                    {
                        return first.Substring(startIndex, candidateLength - startIndex);
                    }
                }
            }

            return string.Empty;
        }
    }
}
