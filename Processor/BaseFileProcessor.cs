using DataProcessor.Models;
using Npgsql;

namespace DataProcessor.Processor;
public abstract class BaseFileProcessor
{
    private static string ConnectionString = "Host=localhost;Username=congress_app;Password=database_senha;Database=congress_db";
    private readonly NpgsqlConnection conn;

    public BaseFileProcessor()
    {
        conn = new NpgsqlConnection(ConnectionString);
        conn.Open();
    }

    public NpgsqlConnection Connection
    {
        get
        {
            return conn;
        }
    }

    protected void StoreFileInfo(FileMetadata metadata)
    {
        string fileInfoUpdateOrInsert = @"
                        INSERT INTO data_files (file_name, content_length, etag, processing_datetime)
                        VALUES (@fileName, @contentLength, @etag, @date)
                        ON CONFLICT (file_name) DO UPDATE
                        SET content_length = @contentLength,
                            etag = @etag,
                            processing_datetime = @date";
        var categoriaCommand = new NpgsqlCommand(fileInfoUpdateOrInsert, conn);
        categoriaCommand.Parameters.AddWithValue("fileName", metadata.Name);
        categoriaCommand.Parameters.AddWithValue("contentLength", metadata.ContentLength!);
        categoriaCommand.Parameters.AddWithValue("etag", metadata.ETag);
        categoriaCommand.Parameters.AddWithValue("date", DateTime.Now);
        categoriaCommand.ExecuteNonQuery();
    }

    public bool ShouldProcessThisFile(FileMetadata newFileInfo)
    {
        FileMetadata? lastProcessedFile = GetLastProcessedFile(newFileInfo.Name);
        if (lastProcessedFile == null)
        {
            return true;
        }

        return (newFileInfo.ETag != lastProcessedFile.ETag &&
                newFileInfo.ContentLength > lastProcessedFile.ContentLength);
    }

    private FileMetadata? GetLastProcessedFile(string fileName)
    {
        string query = @"
        SELECT content_length, etag FROM data_files
        WHERE file_name = @fileName";

        using (var queryCommand = new NpgsqlCommand(query, conn))
        {
            queryCommand.Parameters.AddWithValue("fileName", fileName);
            var reader = queryCommand.ExecuteReader();
            if (!reader.HasRows)
            {
                reader.DisposeAsync();
                return null;
            }

            reader.Read();
            long contentLength = reader.GetInt64(0);
            string etag = reader.GetString(1);
            //DateTime dateFile = reader.GetDateTime(2);
            reader.DisposeAsync();
            FileMetadata fileInfo = new FileMetadata()
            {
                ContentLength = contentLength,
                ETag = etag
            };

            return fileInfo;
        }
    }
}