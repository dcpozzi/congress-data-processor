// See https://aka.ms/new-console-template for more information
//await ImportData.ImportDeputados();
//await ImportData.Main();

using DataProcessor;
using DataProcessor.Processor;
using Newtonsoft.Json.Linq;

public class Startup
{
    public static void Init()
    {
        string url = "https://www.camara.leg.br/cotas/Ano-2023.json.zip";
        // FileGetter getter = new FileGetter();
        // getter.GetMetadata(url, "Ano-2023.json.zip");
        FilePreparer file = new FilePreparer();
        try
        {
            JObject data = file.GetJSON(url);
            new ImportData().Execute(data);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
        finally
        {
            Console.WriteLine("Deleting files...");
            file.CleanUpFiles();
        }
    }
}

