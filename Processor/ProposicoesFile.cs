using DataProcessor.Models;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace DataProcessor.Processor;

public class PorposicoesFileProcessor : BaseFileProcessor
{
    public const string FILE_URL = "https://dadosabertos.camara.leg.br/arquivos/proposicoes/json/proposicoes-2023.json";
    private const string URL_PROPOSICAO = "https://dadosabertos.camara.leg.br/api/v2/proposicoes/{0}/autores";
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
            //Console.WriteLine(item);
            int idProposicao = item["id"].ToObject<int>();
            string ementa = item["ementa"].ToObject<string>();
            string truncatedEmenta = ementa.Substring(0, Math.Min(ementa.Length, 1000));
            StoreProposicao(idProposicao, truncatedEmenta);
            UpdateStatistics();
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
    private void StoreAutoresFromProposicao(int idProposicao)
    {
        string urlProposicao = string.Format(URL_PROPOSICAO, idProposicao);
        JObject data = JObject.Parse(GetJsonFromUrl(urlProposicao));
        //Console.WriteLine(data);
        foreach (var item in data["dados"])
        {
            //Console.WriteLine(item["uri"].ToObject<string>());
            int idDeputado = ExtrairIdDeputadoFromUrl(item["uri"].ToObject<string>());
            if (!DeputadoExists(idDeputado))
            {
                continue;
            }

            bool proponente = item["proponente"].ToObject<bool>();
            StoreAutoresFromProposicao(idProposicao, idDeputado, proponente);
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

    private int ExtrairIdDeputadoFromUrl(string url)
    {
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