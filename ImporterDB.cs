using System;
using System.Data;
using Npgsql;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Threading.Tasks;

public class ImportData
{
    private static string ConnectionString = "Host=localhost;Username=congress_app;Password=database_senha;Database=congress_db";
    private NpgsqlConnection conn = null;
    private HashSet<String> updatedDeputados = new HashSet<String>();
    private Dictionary<String, int> deputadosAPI = new Dictionary<string, int>();
    private int registerImported = 0;
    public void Execute(JObject jsonData)
    {
        //var jsonData = JObject.Parse(File.ReadAllText("./Ano-2023.json"));
        Console.WriteLine("Starting...");
        conn = new NpgsqlConnection(ConnectionString);
        conn.Open();
        using (var transaction = conn.BeginTransaction())
        {
            ClearDataBase();
            ImportDeputados();
            LoadDeputadosAPI();

            ProcessData(jsonData, transaction);
        }

        Console.WriteLine("Data imported successfully");
    }

    private void ProcessData(JObject jsonData, NpgsqlTransaction transaction)
    {
        int idDocumento = -1;
        try
        {
            foreach (var item in jsonData["dados"])
            {
                idDocumento = ProcessItem(item);
            }

            transaction.Commit();
        }
        catch (Exception e)
        {
            transaction.Rollback();
            Console.Error.WriteLine($"Error importing data id ({idDocumento}): {e}");
        }
    }

    private int ProcessItem(JToken item)
    {
        int idDocumento = item["idDocumento"].ToObject<int>();
        int? fornecedorId = ProcessFornecedor(item);
        int? categoriaId = ProcessCategoria(item);
        int idDeputado = ProcessDeputado(item);
        ProcessGasto(idDocumento, item, fornecedorId, categoriaId, idDeputado);
        UpdateStatistics();
        return idDocumento;
    }

    private void UpdateStatistics()
    {
        registerImported++;
        if (registerImported % 100 == 0)
        {
            Console.WriteLine($"Registers: {registerImported}");
        }
    }

    private void ProcessGasto(int idDocumento, JToken item, int? fornecedorId, int? categoriaId, int idDeputado)
    {
        string gastoQuery = @"
                            INSERT INTO gastos (id_documento, id_deputado, categoria_gasto_id, fornecedor_id, numero, data_emissao, valor_documento, valor_glosa, valor_liquido)
                            VALUES (@idDocumento, @idDeputado, @categoriaId, @fornecedorId, @numero, @dataEmissao, @valorDocumento, @valorGlosa, @valorLiquido)";
        var gastoCommand = new NpgsqlCommand(gastoQuery, conn);
        gastoCommand.Parameters.AddWithValue("idDocumento", idDocumento);
        gastoCommand.Parameters.AddWithValue("idDeputado", idDeputado);
        gastoCommand.Parameters.AddWithValue("categoriaId", (object)categoriaId ?? DBNull.Value);
        gastoCommand.Parameters.AddWithValue("fornecedorId", (object)fornecedorId ?? DBNull.Value);
        gastoCommand.Parameters.AddWithValue("numero", item["numero"].ToObject<string>());
        gastoCommand.Parameters.AddWithValue("dataEmissao", item["dataEmissao"].ToString() == "" ? DBNull.Value : (object)item["dataEmissao"].ToObject<DateTime>());
        gastoCommand.Parameters.AddWithValue("valorDocumento", item["valorDocumento"].ToObject<decimal>());
        gastoCommand.Parameters.AddWithValue("valorGlosa", item["valorGlosa"].ToObject<decimal>());
        gastoCommand.Parameters.AddWithValue("valorLiquido", item["valorLiquido"].ToObject<decimal>());
        gastoCommand.ExecuteNonQuery();
    }

    private int ProcessDeputado(JToken item)
    {
        int idDeputadoArq = item["numeroDeputadoID"].ToObject<int>();
        String nomeDeputado = item["nomeParlamentar"].ToObject<String>();
        int idDeputado = 0;

        if (deputadosAPI.ContainsKey(nomeDeputado))
        {
            idDeputado = deputadosAPI[nomeDeputado];

            if (!updatedDeputados.Contains(nomeDeputado))
            {
                if (UpdateDeputados(idDeputado, idDeputadoArq))
                {
                    updatedDeputados.Add(nomeDeputado);
                    Console.WriteLine($"Deputado {nomeDeputado} atualizado na base.");
                }
            }
        }
        else
        {
            idDeputado = InsertDeputado(nomeDeputado, idDeputadoArq);
            deputadosAPI.Add(nomeDeputado, idDeputado);
            updatedDeputados.Add(nomeDeputado);
            Console.WriteLine($"Deputado {nomeDeputado} inserido na base.");
        }

        return idDeputado;
    }

    private int? ProcessCategoria(JToken item)
    {
        string categoriaQuery = @"
                        INSERT INTO categorias_gastos (descricao)
                        VALUES (@descricao)
                        ON CONFLICT (descricao) DO UPDATE
                        SET descricao = excluded.descricao
                        RETURNING id";
        var categoriaCommand = new NpgsqlCommand(categoriaQuery, conn);
        categoriaCommand.Parameters.AddWithValue("descricao", item["descricao"].ToObject<string>());
        int? categoriaId = (int?)categoriaCommand.ExecuteScalar();
        return categoriaId;
    }

    private int? ProcessFornecedor(JToken item)
    {
        string fornecedorQuery = @"
                            INSERT INTO fornecedores (nome, cnpjCPF)
                            VALUES (@nome, @cnpjCPF)
                            ON CONFLICT (cnpjCPF) DO UPDATE
                            SET nome = excluded.nome, cnpjCPF = excluded.cnpjCPF
                            RETURNING id";
        var fornecedorCommand = new NpgsqlCommand(fornecedorQuery, conn);
        fornecedorCommand.Parameters.AddWithValue("nome", item["fornecedor"].ToObject<string>());
        fornecedorCommand.Parameters.AddWithValue("cnpjCPF", item["cnpjCPF"].ToString().Trim());
        int? fornecedorId = (int?)fornecedorCommand.ExecuteScalar();
        return fornecedorId;
    }

    private int InsertDeputado(string nomeDeputado, int idDeputadoArq)
    {
        string updateDeputado = @"
            INSERT INTO deputados (nome, id_deputado_arq)
                VALUES (@nome, @idDeputadoArq)
                RETURNING id";
        var updateCommand = new NpgsqlCommand(updateDeputado, conn);
        updateCommand.Parameters.AddWithValue("nome", nomeDeputado);
        updateCommand.Parameters.AddWithValue("idDeputadoArq", idDeputadoArq);
        int idDeputado = (int)updateCommand.ExecuteScalar();
        return idDeputado;
    }

    private bool UpdateDeputados(int idDeputado, int idDeputadoArq)
    {
        string updateDeputado = @"
            update deputados
            set id_deputado_arq = @idDeputadoArq
            where id = @idDeputado";
        var updateCommand = new NpgsqlCommand(updateDeputado, conn);
        updateCommand.Parameters.AddWithValue("idDeputadoArq", idDeputadoArq);
        updateCommand.Parameters.AddWithValue("idDeputado", idDeputado);
        int? linesAffected = (int?)updateCommand.ExecuteNonQuery();
        return (linesAffected > 0);
    }

    private void ImportDeputados()
    {
        var jsonObject = JObject.Parse(File.ReadAllText("./deputados.json"));
        var deputados = jsonObject["dados"].ToObject<JArray>();


        try
        {
            foreach (var deputado in deputados)
            {
                int idDeputadoAPI = deputado.Value<int>("id");
                string nome = deputado.Value<string>("nome");

                string query = @"
                        INSERT INTO deputados (id_deputado_api, nome)
                        VALUES (@idDeputadoAPI, @nome)";

                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("idDeputadoAPI", idDeputadoAPI);
                    cmd.Parameters.AddWithValue("nome", nome);

                    cmd.ExecuteNonQuery();
                }
            }

        }
        catch (Exception ex)
        {
            //Console.WriteLine("Error importing data: " + ex.Message);
            throw;
        }
    }
    private void LoadDeputadosAPI()
    {
        string query = @"
        SELECT id, nome FROM deputados";

        deputadosAPI = new Dictionary<string, int>();
        using (var queryCommand = new NpgsqlCommand(query, conn))
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
    }

    private void ClearDataBase()
    {
        string clearQuery = @"
            delete from gastos;
            delete from categorias_gastos;
            delete from fornecedores;
            delete from deputados;";

        var clearCommand = new NpgsqlCommand(clearQuery, conn);
        int? linesDeleted = (int?)clearCommand.ExecuteNonQuery();
        Console.WriteLine($"Deputados excluídos: {linesDeleted}");
    }
}


