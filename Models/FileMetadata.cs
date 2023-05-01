namespace DataProcessor.Models;

public class FileMetadata
{
    public string Name { get; set; } = String.Empty;
    public string ETag { get; set; } = String.Empty;
    public long? ContentLength { get; set; } = 0;
}