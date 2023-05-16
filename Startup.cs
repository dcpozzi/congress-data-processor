using DataProcessor;
using DataProcessor.Models;
using DataProcessor.Processor;
using Newtonsoft.Json.Linq;

public class Startup
{
    public static void Init()
    {
        ProcessFileDespesas();
        ProcessFileProposicoes();
        ProcessFileAutoresProposicoes();
    }

    private static void ProcessFileDespesas()
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

