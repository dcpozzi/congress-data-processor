using DataProcessor.Models;

namespace DataProcessor.Processor;

public class PorposicoesFileProcessor : BaseFileProcessor
{
    public const string FILE_URL = "https://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/json/proposicoesAutores-2023.json";
    public void Execute(FileMetadata metadata)
    {
        //Clean up database

        //For each Proposicao
        /*Store in the database:
            uri
            ementa
            Buscar autores ementa
                https://dadosabertos.camara.leg.br/api/v2/proposicoes/2345506/autores

            table Proposicao
                id
                uri
                ementa

            table Deputado_Proposicao
                deputado_id
                proposicao_id
                proponente (true|false)

        */

    }
}