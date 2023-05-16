using CongressoDataProcessor.Models;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace CongressoDataProcessor.Processor;

public class ProposicoesFileProcessor : BaseFileProcessor
{
    private const string FILE_URL = "https://dadosabertos.camara.leg.br/arquivos/proposicoes/json/proposicoes-2023.json";
    private int registerImported;

    public override string FileUrl => FILE_URL;
    public override void Execute(FileMetadata metadata)
    {
        using (var transaction = this.Connection.BeginTransaction())
        {
            try
            {
                ClearRegisters();
                ProcessRegisters(metadata.Data);
                StoreFileInfo(metadata);
                transaction.Commit();
            }
            catch (Exception e)
            {
                transaction.Rollback();
                Console.Error.WriteLine(e.ToString());
            }
        }
    }

    private void ProcessRegisters(JObject data)
    {
        ValidateData(data);

        foreach (var item in data["dados"]!)
        {
            ValidateAndStoreProposicao(item);
        }
    }

    private void ValidateAndStoreProposicao(JToken item)
    {
        if (item["id"] == null || item["ementa"] == null)
        {
            return;
        }

        int idProposicao = item["id"]!.ToObject<int>();
        string ementa = item["ementa"]!.ToObject<string>()!;
        string truncatedEmenta = ementa.Substring(0, Math.Min(ementa.Length, 1000));
        StoreProposicao(idProposicao, truncatedEmenta);
        UpdateStatistics();
    }

    private static void ValidateData(JObject data)
    {
        if (data["dados"] == null)
        {
            throw new Exception("Invalid file");
        }
    }

    private void UpdateStatistics()
    {
        registerImported++;
        if (registerImported % 100 == 0)
        {
            Console.WriteLine($"Registers: {registerImported}");
        }
    }

    private void StoreProposicao(int idProposicao, string ementa)
    {
        string sql = "INSERT INTO proposicoes (id_proposicao, ementa) VALUES (@id_proposicao, @ementa)";
        using var cmd = new NpgsqlCommand(sql, Connection);
        cmd.Parameters.AddWithValue("id_proposicao", idProposicao);
        cmd.Parameters.AddWithValue("ementa", ementa);
        cmd.ExecuteNonQuery();
    }

    private void ClearRegisters()
    {
        ClearDeputadoProposicao();
        ClearProposicao();
    }

    private void ClearDeputadoProposicao()
    {
        string sql = "DELETE FROM deputados_proposicoes";
        var cmd = new NpgsqlCommand(sql, Connection);
        cmd.ExecuteNonQuery();
    }

    private void ClearProposicao()
    {
        string sql = "DELETE FROM proposicoes";
        var cmd = new NpgsqlCommand(sql, Connection);
        cmd.ExecuteNonQuery();
    }
}