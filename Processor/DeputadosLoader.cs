using Newtonsoft.Json.Linq;
using Npgsql;

namespace CongressoDataProcessor.Processor;

public class DeputadosLoader
{
    private const string URL = "https://dadosabertos.camara.leg.br/api/v2/deputados";
    private readonly NpgsqlConnection connection;

    public DeputadosLoader(NpgsqlConnection connection)
    {
        this.connection = connection;
    }

    public void Import()
    {
        ClearRegisters();
        ProcessRegisters();
    }

    private void ProcessRegisters()
    {
        JObject data = GetData();
        foreach (var item in data["dados"]!)
        {
            StoreDeputado(item);
        }
    }

    private static JObject GetData()
    {
        string json = NetworkAccess.GetJsonFromUrl(URL);
        JObject data = JObject.Parse(json);
        return data;
    }

    private void StoreDeputado(JToken deputado)
    {
        int idDeputadoAPI = deputado.Value<int>("id");
        string nome = deputado.Value<string>("nome")!;

        string query = @"
                        INSERT INTO deputados (id_deputado_api, nome)
                        VALUES (@idDeputadoAPI, @nome)";

        using (var cmd = new NpgsqlCommand(query, this.connection))
        {
            cmd.Parameters.AddWithValue("idDeputadoAPI", idDeputadoAPI);
            cmd.Parameters.AddWithValue("nome", nome);

            cmd.ExecuteNonQuery();
        }
    }

    public void ClearRegisters()
    {
        string query = @"DELETE FROM deputados";

        using (var cmd = new NpgsqlCommand(query, this.connection))
        {
            cmd.ExecuteNonQuery();
        }
    }

    public Dictionary<string, int> GetDeputados()
    {
        string query = @"
        SELECT id, nome FROM deputados";

        var deputadosAPI = new Dictionary<string, int>();
        using (var queryCommand = new NpgsqlCommand(query, this.connection))
        {
            using (var reader = queryCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int id = reader.GetInt32(0);
                    string nome = reader.GetString(1);
                    deputadosAPI[nome] = id;
                }
            }
        }
        return deputadosAPI;
    }

    public int InsertDeputado(string nomeDeputado, int idDeputadoArq)
    {
        string updateDeputado = @"
            INSERT INTO deputados (nome, id_deputado_arq)
                VALUES (@nome, @idDeputadoArq)
                RETURNING id";
        var updateCommand = new NpgsqlCommand(updateDeputado, this.connection);
        updateCommand.Parameters.AddWithValue("nome", nomeDeputado);
        updateCommand.Parameters.AddWithValue("idDeputadoArq", idDeputadoArq);
        int? idDeputado = (int?)updateCommand.ExecuteScalar();
        return idDeputado ?? 0;
    }

    public bool UpdateDeputados(int idDeputado, int idDeputadoArq)
    {
        string updateDeputado = @"
            update deputados
            set id_deputado_arq = @idDeputadoArq
            where id = @idDeputado";
        var updateCommand = new NpgsqlCommand(updateDeputado, this.connection);
        updateCommand.Parameters.AddWithValue("idDeputadoArq", idDeputadoArq);
        updateCommand.Parameters.AddWithValue("idDeputado", idDeputado);
        int? linesAffected = (int?)updateCommand.ExecuteNonQuery();
        return (linesAffected > 0);
    }

}
