using DataProcessor.Models;

namespace DataProcessor;

public class FileGetter
{
    public FileMetadata GetMetadata(string url, string name)
    {
        using var httpClient = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Head, url);
        using var response = httpClient.Send(request);
        response.EnsureSuccessStatusCode();

        FileMetadata metadata = new FileMetadata()
        {
            ETag = response.Headers.ETag!.Tag,
            Name = name,
            ContentLength = response.Content.Headers.ContentLength
        };
        //metadata.Etag = response.Headers...Tag.ToString();
        // foreach (var header in response.Headers)
        // {
        //     Console.WriteLine($"{header.Key}: {string.Join(",", header.Value)}");
        // }
        return metadata;
    }



}