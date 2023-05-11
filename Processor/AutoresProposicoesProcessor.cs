using DataProcessor.Models;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace DataProcessor.Processor;

public class AutoresProposicoesProcessor : BaseFileProcessor
{
    public const string FILE_URL = "https://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/json/proposicoesAutores-2023.json";
    private int registerImported;

    public void Execute(FileMetadata metadata)
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
        foreach (var item in data["dados"])
        {
            int? idDeputado = ExtrairIdDeputadoFromUrl(item.Value<string>("uriAutor"));
            if (idDeputado == null)
            {
                continue;
            }

            if (!DeputadoExists(idDeputado.Value))
            {
                continue;
            }

            int idProposicao = item.Value<int>("idProposicao");
            if (!ProposicaoExists(idProposicao))
            {
                continue;
            }

            StoreAutoresFromProposicao(
                idProposicao: idProposicao,
                idDeputado: idDeputado.Value,
                proponente: item.Value<int>("proponente") == 1 ? true : false);
            UpdateStatistics();
        }
    }

    private bool ProposicaoExists(int idProposicao)
    {
        string sql = "SELECT COUNT(*) FROM proposicoes WHERE id_proposicao = @id_proposicao";
        using var cmd = new NpgsqlCommand(sql, Connection);
        cmd.Parameters.AddWithValue("id_proposicao", idProposicao);
        int count = Convert.ToInt32(cmd.ExecuteScalar());
        return (count > 0);
    }

    private void UpdateStatistics()
    {
        registerImported++;
        if (registerImported % 100 == 0)
        {
            Console.WriteLine($"Registers: {registerImported}");
        }
    }

    private bool DeputadoExists(int idDeputado)
    {
        string sql = "SELECT COUNT(*) FROM deputados WHERE id_deputado_api = @id_deputado";
        using var cmd = new NpgsqlCommand(sql, Connection);
        cmd.Parameters.AddWithValue("id_deputado", idDeputado);
        int count = Convert.ToInt32(cmd.ExecuteScalar());
        return (count > 0);
    }

    private int? ExtrairIdDeputadoFromUrl(string url)
    {
        if (!url.Contains("deputados"))
        {
            return null;
        }
        string[] splittedUrl = url.Split('/');
        string idDeputado = splittedUrl[splittedUrl.Length - 1];
        return int.Parse(idDeputado);
    }

    private void StoreAutoresFromProposicao(int idProposicao, int idDeputado, bool proponente)
    {
        string sql = "INSERT INTO deputados_proposicoes (id_deputado, id_proposicao, proponente) VALUES (@id_deputado, @id_proposicao, @proponente)";
        using var cmd = new NpgsqlCommand(sql, Connection);
        cmd.Parameters.AddWithValue("id_deputado", idDeputado);
        cmd.Parameters.AddWithValue("id_proposicao", idProposicao);
        cmd.Parameters.AddWithValue("proponente", proponente);
        cmd.ExecuteNonQuery();
    }

    private string GetJsonFromUrl(string url)
    {
        using (HttpClient client = new HttpClient())
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Add("Accept", "application/json");
            using var response = client.Send(request);
            response.EnsureSuccessStatusCode();
            string responseBody = response.Content.ReadAsStringAsync().Result;
            return responseBody;
        }
    }
    private void ClearRegisters()
    {
        ClearDeputadoProposicao();
    }

    private void ClearDeputadoProposicao()
    {
        string sql = "DELETE FROM deputados_proposicoes";
        var cmd = new NpgsqlCommand(sql, Connection);
        cmd.ExecuteNonQuery();
    }
}