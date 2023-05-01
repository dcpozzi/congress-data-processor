using System.IO.Compression;
using Newtonsoft.Json.Linq;

namespace DataProcessor.Processor;

public class FilePreparer
{
    private string zipPath = "downloaded.zip";
    private string extractedPath = "extracted";
    private string jsonFileName = string.Empty;

    public JObject GetJSON(string url)
    {
        SaveZipFromUrl(url);
        ExtractingFileFromZip();
        return GetContentFile(jsonFileName);
    }

    private string SaveZipFromUrl(string url)
    {
        var zipPath = "downloaded.zip";

        using (var httpClient = new HttpClient())
        {
            using (var response = httpClient.GetAsync(url).GetAwaiter().GetResult())
            {
                response.EnsureSuccessStatusCode();
                using (var fileStream = new FileStream(zipPath, FileMode.Create, FileAccess.Write))
                {
                    response.Content.CopyToAsync(fileStream);
                }
            }
        }
        return zipPath;
    }

    private void ExtractingFileFromZip()
    {
        ZipFile.ExtractToDirectory(zipPath, extractedPath);
        string[] files = Directory.GetFiles(extractedPath);
        if (files.Length == 0)
        {
            throw new FileNotFoundException("Extracted file from zip not found");
        }

        jsonFileName = files[0];
    }

    private JObject GetContentFile(string fileName)
    {
        var jsonStream = File.ReadAllText(jsonFileName);

        try
        {
            var jsonDocument = JObject.Parse(jsonStream);
            return jsonDocument;
        }
        catch
        {
            throw;
        }
    }

    public void CleanUpFiles()
    {
        File.Delete(zipPath);
        Directory.Delete(extractedPath, true);
    }
}