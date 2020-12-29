using Knapcode.BlobDelta.Test.Functional;
using Microsoft.WindowsAzure.Storage;
using Xunit;

namespace Knapcode.BlobDelta.Test.Support
{
    public class NoEmulatorFactAttribute : FactAttribute
    {
        public NoEmulatorFactAttribute()
        {
            if (CloudStorageAccount.Parse(BaseBlobStorageFacts.GetConnectionString()) == CloudStorageAccount.DevelopmentStorageAccount)
            {
                Skip = "This test can't run on the Azure Storage Emulator.";
            }
        }
    }
}
