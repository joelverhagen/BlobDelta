namespace Knapcode.TableDelta
{
    public enum EntityComparisonType
    {
        Same,
        MissingFromLeft,
        MissingFromRight,
        DisjointProperties,
        DifferentPropertiesValues,
    }
}
