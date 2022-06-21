using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

try
{
	Console.WriteLine("Enter the container name prefix to purge");
	var preFix = Console.ReadLine();
	
	var service = new BlobServiceClient("endpoint-here");
	var container = service.GetBlobContainerClient("container-name-here");
	var batch = service.GetBlobBatchClient();
	var enumerator = container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix: preFix).GetAsyncEnumerator();
	var items = new List<Uri>();
	long blobCounter = 0;
	const int max_items = 256;
	
	Console.WriteLine($"starting discovery of container using prefix {preFix}");
	while(await enumerator.MoveNextAsync())
	{
		try
		{
			items.Add(new Uri($"{container.Uri}/{enumerator.Current.Name}"));
			var currentCount = items.Count;
			if (currentCount >= max_items)
			{
				blobCounter += Math.Min(max_items, currentCount);
				Console.WriteLine($"Deleted {blobCounter.ToString()} total blobs");
				batch.DeleteBlobsAsync(items); // using await is much slower here
				items.Clear();
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"{ex.Message}:{enumerator.Current.Name}");
		}
	}
	await batch.DeleteBlobsAsync(items);
}
catch (RequestFailedException e)
{
	Console.WriteLine(e.Message);
}

Console.WriteLine("Done");
Console.ReadLine();