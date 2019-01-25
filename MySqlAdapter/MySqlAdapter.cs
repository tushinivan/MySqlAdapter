using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Extensions.MySql
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
        public int TimeOut = 300;

        /// <summary>
        /// Максимальное время выполнения запроса. По умолчанию 3600 сек.
        /// </summary>
        public int MaximumTimeOut = 3600;

        private int runningQueries;
        private readonly string connectionString;

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

        
        public int Execute(string query, int timeOut)
        {
            int result = -1;

            result = _Execute(query, true, (connection) => 
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = timeOut >= 0 ? timeOut : TimeOut;

                    return command.ExecuteNonQuery();
                }
            });

            return result;
        }
        public DataTable Select(string query, int timeOut)
        {
            DataTable result = null;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    adapter.SelectCommand.CommandTimeout = timeOut >= 0 ? timeOut : TimeOut;

                    result = new DataTable();
                    adapter.Fill(result);

                    return result;
                }
            });

            return result;
        }
        public DataSet SelectDataSet(string query, int timeOut)
        {
            DataSet result = null;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    adapter.SelectCommand.CommandTimeout = timeOut >= 0 ? timeOut : TimeOut;

                    result = new DataSet();
                    adapter.Fill(result);

                    return result;
                }
            });

            return result;
        }
        public DataRow SelectRow(string query, int timeOut)
        {
            DataRow result = null;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    adapter.SelectCommand.CommandTimeout = timeOut >= 0 ? timeOut : TimeOut;

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
        public ScalarResult<T> Select<T>(string query, int timeOut)
        {
            ScalarResult<T> result = null;

            result = _Execute(query, true, (connection) =>
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.CommandTimeout = timeOut >= 0 ? timeOut : TimeOut;

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

        private T _Execute<T>(string query, bool loopQuery, Func<MySqlConnection, T> func)
        {
            Interlocked.Increment(ref runningQueries);

            T result = default(T);
            do
            {
                bool hasError = false;
                try
                {
                    using (MySqlConnection connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();
                        result = func(connection);
                        break;
                    }
                }
                catch (Exception ex)
                {
                    hasError = true;
                }

                if (hasError && loopQuery)
                {
                    Thread.Sleep(LoopTimeOut);//делаем паузу в запросах
                }
                else
                {
                    break;//выходим из цикла обработки ошибок
                }
            } while (loopQuery);

            Interlocked.Decrement(ref runningQueries);
            return result;
        }
    }

    public class ScalarResult<T>
    {
        public bool DbNull;
        public T Value;
    }
}
