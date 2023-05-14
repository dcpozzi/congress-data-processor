using System.IO.Compression;
using DataProcessor.Models;
using Newtonsoft.Json.Linq;

namespace DataProcessor.Processor;

public class FileManager
{
    private string extractedPath = "extracted";
    private FileMetadata metadata;
    private readonly string url;

    private readonly string fileName;

    public FileManager(string url)
    {
        this.url = url;
        fileName = ExtractFileName();
        var response = NetworkAccess.GetHeader(url);
        metadata = ExtractHeaders(response);
    }

    public FileMetadata GetMetadata()
    {

        return metadata;
    }

    private FileMetadata ExtractHeaders(HttpResponseMessage response)
    {
        return new FileMetadata()
        {
            ETag = response.Headers.ETag!.Tag,
            Name = ExtractFileName(),
            ContentLength = response.Content.Headers.ContentLength
        };
    }

    private string ExtractFileName()
    {
        var splittedUrl = url.Split('/');
        return splittedUrl[splittedUrl.Length - 1];
    }

    public FileMetadata GetFile()
    {
        FileMetadata fileInfo = SaveFileFromUrl();
        string jsonFileName = this.fileName;
        if (fileName.ToLower().EndsWith(".zip"))
        {
            jsonFileName = ExtractingFileFromZip();
        }
        fileInfo.Data = GetContentFile(jsonFileName);
        return fileInfo;
    }

    private FileMetadata SaveFileFromUrl()
    {
        NetworkAccess.SaveFileFromUrl(url, fileName);
        return metadata;
    }

    private string ExtractingFileFromZip()
    {
        ZipFile.ExtractToDirectory(fileName, extractedPath);
        string[] files = Directory.GetFiles(extractedPath);
        if (files.Length == 0)
        {
            throw new FileNotFoundException("Extracted file from zip not found");
        }

        return files[0];
    }

    private JObject GetContentFile(string fileName)
    {
        var jsonStream = File.ReadAllText(fileName);

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
        File.Delete(fileName);
        if (Directory.Exists(extractedPath))
        {
            Directory.Delete(extractedPath, true);
        }
    }
}