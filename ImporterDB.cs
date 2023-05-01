using System;
using System.Data;
using Npgsql;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Threading.Tasks;
using DataProcessor.Models;

public class ImportData
{
    private static string ConnectionString = "Host=localhost;Username=congress_app;Password=database_senha;Database=congress_db";
    private HashSet<String> updatedDeputados = new HashSet<String>();
    private Dictionary<String, int> deputadosAPI = new Dictionary<string, int>();
    private int registerImported = 0;
    private readonly NpgsqlConnection conn;

    public ImportData()
    {
        conn = new NpgsqlConnection(ConnectionString);
        conn.Open();
    }

    public void Execute(FileMetadata metadata)
    {
        Console.WriteLine("Starting...");
        using (var transaction = conn.BeginTransaction())
        {
            try
            {
                ClearDataBase();
                ImportDeputados();
                LoadDeputadosAPI();

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

        Console.WriteLine("Data imported successfully");
    }

    private void StoreFileInfo(FileMetadata metadata)
    {
        string fileInfoUpdateOrInsert = @"
                        INSERT INTO data_files (file_name, content_length, etag, processing_datetime)
                        VALUES (@fileName, @contentLength, @etag, @date)
                        ON CONFLICT (file_name) DO UPDATE
                        SET content_length = @contentLength,
                            etag = @etag,
                            processing_datetime = @date";
        var categoriaCommand = new NpgsqlCommand(fileInfoUpdateOrInsert, conn);
        categoriaCommand.Parameters.AddWithValue("fileName", metadata.Name);
        categoriaCommand.Parameters.AddWithValue("contentLength", metadata.ContentLength);
        categoriaCommand.Parameters.AddWithValue("etag", metadata.ETag);
        categoriaCommand.Parameters.AddWithValue("date", DateTime.Now);
        categoriaCommand.ExecuteNonQuery();
    }

    public bool ShouldProcessThisFile(FileMetadata newFileInfo)
    {
        FileMetadata? lastProcessedFile = GetLastProcessedFile(newFileInfo.Name);
        if (lastProcessedFile == null)
        {
            return true;
        }

        return (newFileInfo.ETag != lastProcessedFile.ETag &&
                newFileInfo.ContentLength > lastProcessedFile.ContentLength);
    }

    private FileMetadata? GetLastProcessedFile(string fileName)
    {
        string query = @"
        SELECT content_length, etag FROM data_files
        WHERE file_name = @fileName";

        using (var queryCommand = new NpgsqlCommand(query, conn))
        {
            queryCommand.Parameters.AddWithValue("fileName", fileName);
            var reader = queryCommand.ExecuteReader();
            if (!reader.HasRows)
            {
                reader.DisposeAsync();
                return null;
            }

            reader.Read();
            long contentLength = reader.GetInt64(0);
            string etag = reader.GetString(1);
            //DateTime dateFile = reader.GetDateTime(2);
            reader.DisposeAsync();
            FileMetadata fileInfo = new FileMetadata()
            {
                ContentLength = contentLength,
                ETag = etag
            };

            return fileInfo;
        }
    }

    private void ProcessRegisters(JObject jsonData)
    {
        foreach (var item in jsonData["dados"])
        {
            int idDocumento = item["idDocumento"].ToObject<int>();
            int? fornecedorId = ProcessFornecedor(item);
            int? categoriaId = ProcessCategoria(item);
            int idDeputado = ProcessDeputado(item);
            ProcessGasto(idDocumento, item, fornecedorId, categoriaId, idDeputado);
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


