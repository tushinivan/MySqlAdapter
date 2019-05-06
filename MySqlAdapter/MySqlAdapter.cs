using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;

namespace ITsoft.Extensions.MySql
{
    public class MySqlAdapter
    {
        /// <summary>
        /// Выполнять запрос выдавший ошибку, через интервал LoopTimeOut.
        /// </summary>
        public bool LoopQuery = true;

        /// <summary>
        /// Время между повторами запросов в мс. По умолчанию 10000 мс. (10 сек)
        /// </summary>
        public int LoopTimeOut = 10000;

        /// <summary>
        /// Время ожидания выполнения запроса. По умолчанию 300 сек.
        /// </summary>
        public int DefaultTimeOut = 300;

        /// <summary>
        /// Максимальное время выполнения запроса. По умолчанию 3600 сек.
        /// </summary>
        public int MaximumTimeOut = 3600;

        private int runningQueries;
        private readonly string connectionString;

        public delegate void ErrorArgs(Exception ex, string query);
        /// <summary>
        /// Событие происходящее при возникновении ошибки при выполнении запроса.
        /// </summary>
        public event ErrorArgs Error;
        /// <summary>
        /// Событие происходящее при стандартной обработке ошибки выполнения запроса.
        /// </summary>
        public event ErrorArgs ErrorProcessed;

        private Dictionary<int, Action<Exception>> ExceptionHandlers = new Dictionary<int, Action<Exception>>();
        private static Dictionary<string, CacheQuery> caches = new Dictionary<string, CacheQuery>();//кешированные запросы

        public MySqlAdapter(string connectionString)
        {
            if (connectionString != null)
            {
                this.connectionString = connectionString;
            }
            else
            {
                throw new Exception("Не задана строка подключения.");
            }
        }

        public int Execute(string query, int timeOut = -1)
        {
            int result = -1;
            int _timeOut = timeOut >= 0 ? timeOut : DefaultTimeOut;

            result = _Execute(query, true, (connection) => 
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = _timeOut;

                    return command.ExecuteNonQuery();
                }
            });

            return result;
        }
        public DataTable Select(string query, int timeOut = -1)
        {
            DataTable result = null;
            int _timeOut = timeOut >= 0 ? timeOut : DefaultTimeOut;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    adapter.SelectCommand.CommandTimeout = _timeOut;

                    result = new DataTable();
                    adapter.Fill(result);

                    return result;
                }
            });

            return result;
        }
        public DataSet SelectDataSet(string query, int timeOut = -1)
        {
            DataSet result = null;
            int _timeOut = timeOut >= 0 ? timeOut : DefaultTimeOut;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    adapter.SelectCommand.CommandTimeout = _timeOut;

                    result = new DataSet();
                    adapter.Fill(result);

                    return result;
                }
            });

            return result;
        }
        public DataRow SelectRow(string query, int timeOut = -1)
        {
            DataRow result = null;
            int _timeOut = timeOut >= 0 ? timeOut : DefaultTimeOut;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    adapter.SelectCommand.CommandTimeout = _timeOut;

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
        public ScalarResult<T> SelectScalar<T>(string query, int timeOut = -1)
        {
            ScalarResult<T> result = null;
            int _timeOut = timeOut >= 0 ? timeOut : DefaultTimeOut;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.CommandTimeout = _timeOut;

                    object queryResult = command.ExecuteScalar();
                    var tmp = new ScalarResult<T>() { Value = default(T) };

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
        public int SelectReader(string query, Action<object[]> handler, int timeOut = -1)
        {
            int result = -1;
            int _timeOut = timeOut >= 0 ? timeOut : DefaultTimeOut;

            result = _Execute(query, true, (connection) =>
            {
                int counter = 0;
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = _timeOut;

                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        object[] values = new object[reader.FieldCount];
                        reader.GetValues(values);

                        handler(values);
                        counter++;
                    }
                }

                return counter;
            });

            return result;
        }
        private T _Execute<T>(string query, bool loopQuery, Func<MySqlConnection, T> queryFunc)
        {
            Interlocked.Increment(ref runningQueries);

            T result = default(T);
            try
            {
                do
                {
                    try
                    {
                        using (MySqlConnection connection = new MySqlConnection(connectionString))
                        {
                            connection.Open();
                            result = queryFunc(connection);
                            break;
                        }
                    }
                    catch (MySqlException ex)
                    {
                        //получаем обработчик ошибки
                        if (ExceptionHandlers.TryGetValue(ex.Number, out Action<Exception> action))
                        {
                            action(ex);
                            ErrorProcessed?.Invoke(ex, query);
                        }
                        else
                        {
                            //стандартные обработчики ошибок
                            switch (ex.Number)
                            {
                                case 1042://ER_BAD_HOST_ERROR
                                    ErrorProcessed?.Invoke(ex, query);
                                    break;
                                case 1205://ER_LOCK_WAIT_TIMEOUT
                                    ErrorProcessed?.Invoke(ex, query);
                                    break;
                                case 1213://ER_LOCK_DEADLOCK
                                    ErrorProcessed?.Invoke(ex, query);
                                    break;
                                default:
                                    Error?.Invoke(ex, query);
                                    return result;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Error?.Invoke(ex, query);
                        break;
                    }

                    //Если разрешено зацикливание запроса
                    if (loopQuery)
                    {
                        Thread.Sleep(LoopTimeOut);//делаем паузу в запросах
                    }
                } while (loopQuery);
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
            return MySqlHelper.EscapeString(str);
        }
    }

    public class ScalarResult<T>
    {
        public bool DbNull;
        public T Value;
    }
}
