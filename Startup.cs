// See https://aka.ms/new-console-template for more information
//await ImportData.ImportDeputados();
//await ImportData.Main();

using DataProcessor;
using DataProcessor.Models;
using DataProcessor.Processor;
using Newtonsoft.Json.Linq;

public class Startup
{
    public static void Init()
    {
        //ProcessFileExpenses();
        ProcessFileProposicoes();
        ProcessFileAutoresProposicoes();
    }

    private static void ProcessFileExpenses()
    {
        DespesasFileProcessor dataProc = new DespesasFileProcessor();
        FileManager fileManager = new FileManager(DespesasFileProcessor.FILE_URL);
        FileMetadata metadata = fileManager.GetMetadata();
        if (!dataProc.ShouldProcessThisFile(metadata))
        {
            Console.WriteLine("Expenses Database is up to date.");
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

    private static void ProcessFileProposicoes()
    {
        PorposicoesFileProcessor dataProc = new PorposicoesFileProcessor();
        FileManager fileManager = new FileManager(PorposicoesFileProcessor.FILE_URL);
        FileMetadata metadata = fileManager.GetMetadata();
        if (!dataProc.ShouldProcessThisFile(metadata))
        {
            Console.WriteLine("Propositions Database is up to date.");
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

    private static void ProcessFileAutoresProposicoes()
    {
        AutoresProposicoesProcessor dataProc = new AutoresProposicoesProcessor();
        FileManager fileManager = new FileManager(AutoresProposicoesProcessor.FILE_URL);
        FileMetadata metadata = fileManager.GetMetadata();
        if (!dataProc.ShouldProcessThisFile(metadata))
        {
            Console.WriteLine("Propositions Authors Database is up to date.");
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

