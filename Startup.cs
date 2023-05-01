// See https://aka.ms/new-console-template for more information
//await ImportData.ImportDeputados();
//await ImportData.Main();

using DataProcessor;
using DataProcessor.Models;
using DataProcessor.Processor;
using Newtonsoft.Json.Linq;

public class Startup
{
    private const string url = "https://www.camara.leg.br/cotas/Ano-2023.json.zip";

    public static void Init()
    {
        FileManager fileManager = new FileManager(url);
        FileMetadata metadata = fileManager.GetMetadata();
        ImportData dataProc = new ImportData();
        if (!dataProc.ShouldProcessThisFile(metadata))
        {
            Console.WriteLine("Database is up to date.");
            return;
        }

        try
        {
            FileMetadata file = fileManager.GetFile();
            dataProc.Execute(file);
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

