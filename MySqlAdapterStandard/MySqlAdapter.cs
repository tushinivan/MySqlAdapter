using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace ITsoft.Extensions.MySql
{
    public class MySqlAdapter
    {
        /// <summary>
        /// Выполнять запрос выдавший ошибку, через интервал LoopTimeOut.
        /// </summary>
        public bool LoopQuery { get; set; } = true;

        /// <summary>
        /// Время между повторами запросов в мс. По умолчанию 10000 мс. (10 сек)
        /// </summary>
        public int LoopTimeOut { get; set; } = 10000;

        /// <summary>
        /// Время ожидания выполнения запроса. По умолчанию 30 сек.
        /// </summary>
        public int DefaultTimeOut { get; set; } = 30;

        /// <summary>
        /// Максимальное время выполнения запроса. По умолчанию 30 сек.
        /// </summary>
        public int MaximumTimeOut { get; set; } = 300;

        private int runningQueries;
        private readonly string connectionString;

        public delegate void ErrorArgs(Exception ex, QueryContext queryContext);
        /// <summary>
        /// Событие происходящее при возникновении ошибки при выполнении запроса.
        /// </summary>
        public event ErrorArgs Error;
        /// <summary>
        /// Событие происходящее при стандартной обработке ошибки выполнения запроса.
        /// </summary>
        public event ErrorArgs ErrorProcessed;

        private Dictionary<int, Action<Exception, QueryContext>> ExceptionHandlers = new Dictionary<int, Action<Exception, QueryContext>>();
        private static Dictionary<string, CacheQuery> caches = new Dictionary<string, CacheQuery>();//кешированные запросы

        /// <summary>
        /// Connection String.
        /// </summary>
        /// <param name="connectionString"></param>
        public MySqlAdapter(string connectionString)
        {
            if (connectionString != null)
            {
                this.connectionString = connectionString;
                AddDefaultHandlers();
            }
            else
            {
                throw new Exception("Не задана строка подключения.");
            }
        }

        /// <summary>
        /// Create connection from source file.
        /// </summary>
        /// <param name="connectionName">Connection name.</param>
        /// <param name="connectionFile">Source file.</param>
        public MySqlAdapter(string connectionName, string connectionFile)
        {
            if (connectionName == null || connectionName.Length == 0)
            {
                throw new NullReferenceException("Connection name must not null or empty.");
            }

            if (connectionFile == null || connectionFile.Length == 0)
            {
                throw new NullReferenceException("Connection file must not null or empty.");
            }

            Regex regexStrings = new Regex("(?<name>\".+?\"):(?<connectionString>\".+?\")", RegexOptions.IgnoreCase);

            var lines = File.ReadAllLines(connectionFile, Encoding.UTF8);
            foreach (var line in lines)
            {
                var match = regexStrings.Match(line);
                if (match.Success)
                {
                    if (connectionName != null)
                    {
                        if (match.Groups["name"].Value.Trim('"') == connectionName)
                        {
                            connectionString = match.Groups["connectionString"].Value.Trim('"');
                            break;
                        }
                    }
                    else
                    {
                        connectionString = match.Groups["connectionString"].Value.Trim('"');
                        break;
                    }
                }
            }

            if (connectionString == null)
            {
                throw new Exception($"Can not found connection name \"{connectionName}\" in file \"{connectionFile}\"");
            }
            else
            {
                AddDefaultHandlers();
            }
        }

        private void AddDefaultHandlers()
        {
            //ER_BAD_HOST_ERROR
            ExceptionHandlers.Add(1042, null);

            //ER_LOCK_WAIT_TIMEOUT
            ExceptionHandlers.Add(1205, null);

            //ER_LOCK_DEADLOCK
            ExceptionHandlers.Add(1213, null);

            //ER_TIMEOUT
            ExceptionHandlers.Add(0, (ex, context) =>
            {
                if (context.CommandTimeOut < MaximumTimeOut)
                {
                    context.CommandTimeOut = MaximumTimeOut;
                }
                else
                {
                    context.LoopQuery = false;
                    throw ex;
                }
            });
        }

        public int Execute(string query, bool? loopQuery = null, int? timeOut = null)
        {
            int result = -1;

            QueryContext context = new QueryContext()
            {
                Query = query,
                LoopQuery = loopQuery.HasValue ? loopQuery.Value : LoopQuery,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) => 
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    return command.ExecuteNonQuery();
                }
            });

            return result;
        }
        public DataTable Select(string query, bool? loopQuery = null, int? timeOut = null)
        {
            DataTable result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                LoopQuery = loopQuery.HasValue ? loopQuery.Value : LoopQuery,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    result = new DataTable();
                    adapter.Fill(result);

                    return result;
                }
            });

            return result;
        }
        public DataSet SelectDataSet(string query, bool? loopQuery = null, int? timeOut = null)
        {
            DataSet result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                LoopQuery = loopQuery.HasValue ? loopQuery.Value : LoopQuery,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    result = new DataSet();
                    adapter.Fill(result);

                    return result;
                }
            });

            return result;
        }
        public DataRow SelectRow(string query, bool? loopQuery = null, int? timeOut = null)
        {
            DataRow result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                LoopQuery = loopQuery.HasValue ? loopQuery.Value : LoopQuery,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    var tmp = new DataTable();
                    adapter.Fill(tmp);
                    if (tmp.Rows.Count > 0)
                    {
                        return tmp.Rows[0];
                    }

                    return null;
                }
            });

            return result;
        }
        public ScalarResult<T> SelectScalar<T>(string query, bool? loopQuery = null, int? timeOut = null)
        {
            ScalarResult<T> result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                LoopQuery = loopQuery.HasValue ? loopQuery.Value : LoopQuery,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    object queryResult = command.ExecuteScalar();
                    var tmp = new ScalarResult<T>() { Value = default };

                    if (!Convert.IsDBNull(queryResult) && queryResult != null)
                    {
                        var convertType = typeof(T);
                        tmp.Value = (T)Convert.ChangeType(queryResult, convertType);
                    }
                    else
                    {
                        tmp.DbNull = true;
                    }

                    return tmp;
                }
            });

            return result;
        }
        public int SelectReader(string query, Action<Dictionary<string, object>> handler, bool? loopQuery = null, int? timeOut = null)
        {
            int result = -1;

            QueryContext context = new QueryContext()
            {
                Query = query,
                LoopQuery = loopQuery.HasValue ? loopQuery.Value : LoopQuery,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                int counter = 0;
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Dictionary<string, object> row = new Dictionary<string, object>(reader.FieldCount);
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string name = reader.GetName(i);
                            row.Add(name, reader.GetValue(i));
                        }

                        handler?.Invoke(row);

                        counter++;
                    }
                }

                return counter;
            });

            return result;
        }

        private T _Execute<T>(QueryContext queryContext, Func<MySqlConnection, int, T> queryFunc)
        {
            Interlocked.Increment(ref runningQueries);

            T result = default;
            try
            {
                do
                {
                    try
                    {
                        using (MySqlConnection connection = new MySqlConnection(connectionString))
                        {
                            connection.Open();
                            result = queryFunc(connection, queryContext.CommandTimeOut);
                            connection.Close();
                            break;
                        }
                    }
                    catch (MySqlException ex)
                    {
                        //получаем обработчик ошибки
                        if (ExceptionHandlers.TryGetValue(ex.Number, out Action<Exception, QueryContext> action))
                        {
                            action?.Invoke(ex, queryContext);
                            ErrorProcessed?.Invoke(ex, queryContext);
                        }
                        else
                        {
                            Error?.Invoke(ex, queryContext);
                            return result;
                        }
                    }
                    catch (Exception ex)
                    {
                        Error?.Invoke(ex, queryContext);
                        break;
                    }

                    //Если разрешено зацикливание запроса
                    if (queryContext.LoopQuery)
                    {
                        Thread.Sleep(LoopTimeOut);//делаем паузу в запросах
                    }
                } while (queryContext.LoopQuery);
            }
            catch (Exception ex)
            {
                Error?.Invoke(ex, queryContext);
            }
            finally
            {
                Interlocked.Decrement(ref runningQueries);
            }
            return result;
        }

        /// <summary>
        /// Получает кеш запрсов по имени или создает новый.
        /// </summary>
        /// <param name="name">Название</param>
        /// <returns></returns>
        public CacheQuery GetCacheQuery(string name, string query, CacheOptions options)
        {
            CacheQuery result = null;
            lock (caches)
            {
                if (!caches.TryGetValue(name, out result))
                {
                    result = new CacheQuery(this, query, options);
                    caches.Add(name, result);
                }
            }
            return result;
        }

        public DateTime GetServerDateTime()
        {
            var result = SelectScalar<DateTime>("SELECT NOW();");
            if (result != null && !result.DbNull)
            {
                return result.Value;
            }

            throw new Exception("Execute error");
        }
        public static string EscapeString(string str)
        {
            if (str != null)
            {
                return MySqlHelper.EscapeString(str);
            }
            return str;
        }
    }

    public class ScalarResult<T>
    {
        public bool DbNull;
        public T Value;
    }
    public class QueryContext
    {
        public string Query { get; set; }
        public int CommandTimeOut { get; set; }
        public bool LoopQuery { get; set; }
    }
}
