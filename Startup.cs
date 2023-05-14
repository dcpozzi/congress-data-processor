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
        ProcessFileExpenses();
        //ProcessFileProposicoes();
        //ProcessFileAutoresProposicoes();
    }

    private static void ProcessFileExpenses()
    {
        GenericProcess process = new GenericProcess(new DespesasFileProcessor());
        process.Execute();
    }

    private static void ProcessFileProposicoes()
    {
        GenericProcess process = new GenericProcess(new ProposicoesFileProcessor());
        process.Execute();
    }

    private static void ProcessFileAutoresProposicoes()
    {
        GenericProcess process = new GenericProcess(new AutoresProposicoesProcessor());
        process.Execute();
    }
}

