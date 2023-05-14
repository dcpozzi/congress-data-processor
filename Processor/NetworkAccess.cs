namespace DataProcessor.Processor;

public class NetworkAccess
{
    public static HttpResponseMessage GetHeader(string url)
    {
        using var httpClient = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Head, url);
        using var response = httpClient.Send(request);
        response.EnsureSuccessStatusCode();
        return response;
    }

    public static void SaveFileFromUrl(String url, string fileName)
    {
        using (var httpClient = new HttpClient())
        {
            using (var response = httpClient.GetAsync(url).GetAwaiter().GetResult())
            {
                response.EnsureSuccessStatusCode();
                using (var fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write))
                {
                    response.Content.CopyToAsync(fileStream).GetAwaiter().GetResult();
                }
            }
        }
    }

    public static string GetJsonFromUrl(string url)
    {
        using (HttpClient client = new HttpClient())
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Add("Accept", "application/json");
            using var response = client.Send(request);
            response.EnsureSuccessStatusCode();
            string responseBody = response.Content.ReadAsStringAsync().Result;
            return responseBody;
        }
    }
}