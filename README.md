# Delta

Find the difference between two sequences of things. Currently, I've implemented logic for:

- Azure Blob Storage - [Knapcode.BlobDelta](https://www.nuget.org/packages/Knapcode.BlobDelta/)
- Azure Table Storage - [Knapcode.TableDelta](https://www.nuget.org/packages/Knapcode.TableDelta/)
- Azure Search Service - [Knapcode.SearchDelta](https://www.nuget.org/packages/Knapcode.SearchDelta/)

Each of these is a different package.

![Build](https://github.com/joelverhagen/Delta/workflows/Build/badge.svg)

## Example

Here's an example where I use the Blob delta logic to compare my "staging" blob container for my blog to my "production"
container. It uses the Content-MD5 header to compare the blobs so it's pretty fast, albeit serial.

```csharp
var containerLeft = GetContainer("staging", "$web");
var containerRight = GetContainer("prod", "$web");

var enumerableLeft = new BlobEnumerable(containerLeft);
var enumerableRight = new BlobEnumerable(containerRight);

Console.WriteLine($"Left  | {containerLeft.Uri}");
Console.WriteLine($"Right | {containerRight.Uri}");

var blobPairEnumerable = new BlobComparisonEnumerable(
    enumerableLeft,
    enumerableRight);

Console.WriteLine("Finding delta...");
var enumerator = blobPairEnumerable.GetEnumerator();
while (await enumerator.MoveNextAsync())
{
    var paddedType = enumerator.Current.Type.ToString().PadRight(MaxTypeWidth);
    var blobName = enumerator.Current.Left?.Blob.Name ?? enumerator.Current.Right?.Blob.Name;
    Console.WriteLine($"{paddedType} | {blobName}");
}
```

The output looks like this (truncated):

<pre>
Left  | https://staging.blob.core.windows.net/$web
Right | https://prod.blob.core.windows.net/$web
Finding delta...
...
Same                | attachments/blog/2020/11/dynamically-activate-objects-net/ActivatePerf-data.zip
<b>DifferentContent</b>    | attachments/blog/2020/11/dynamically-activate-objects-net/diagram-1.png
Same                | attachments/blog/2020/11/dynamicly-activate-types-net/ActivatePerf-data.zip
Same                | attachments/blog/2020/11/dynamicly-activate-types-net/diagram-1.png
Same                | attachments/blog/2020/12/fastest-net-csv-parsers/BenchmarkDotNet.Artifacts.zip
Same                | attachments/blog/2020/12/fastest-net-csv-parsers/diagram-1.png
Same                | attachments/resume.pdf
DifferentContent    | blog/2009/09/flickr-tree/index.html
DifferentContent    | blog/2010/01/set-v0-5/index.html
DifferentContent    | blog/2010/11/convert-an-int-to-a-string-and-vice-versa-in-c/index.html
...
Same                | img/twitter.png
DifferentContent    | index.html
Same                | keybase.txt
<b>MissingFromLeft</b>     | robots.txt
Same                | sandbox/tests/.htaccess
Same                | sandbox/tests/10MB.zip
...
</pre>