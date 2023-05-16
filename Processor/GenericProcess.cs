using CongressoDataProcessor.Models;

namespace CongressoDataProcessor.Processor;

public class GenericProcess
{
    private readonly BaseFileProcessor fileProcessor;

    public GenericProcess(BaseFileProcessor fileProcessor)
    {
        this.fileProcessor = fileProcessor;
    }

    public void Execute()
    {
        FileManager fileManager = new FileManager(fileProcessor.FileUrl);
        FileMetadata metadata = fileManager.GetMetadata();
        if (!fileProcessor.ShouldProcessThisFile(metadata))
        {
            Console.WriteLine($"{metadata.Name} Database is up to date.");
            return;
        }

        try
        {
            FileMetadata file = fileManager.GetFile();
            fileProcessor.Execute(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
        finally
        {
            Console.WriteLine("Deleting files...");
            Console.WriteLine("Database updated successfully.");
            fileManager.CleanUpFiles();
        }
    }
}