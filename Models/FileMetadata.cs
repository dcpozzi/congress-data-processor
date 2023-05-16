using Newtonsoft.Json.Linq;

namespace CongressoDataProcessor.Models;

public class FileMetadata
{
    public string Name { get; set; } = String.Empty;
    public string ETag { get; set; } = String.Empty;
    public long? ContentLength { get; set; } = 0;
    public JObject Data { get; set; } = new JObject();
}