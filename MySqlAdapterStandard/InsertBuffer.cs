using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ITsoft.Extensions.MySql
{
    /// <summary>
    /// Буфер пакетной вставки.
    /// </summary>
    public class InsertBuffer
    {
        /// <summary>
        /// Количество запросов в очереди.
        /// </summary>
        public int QueueCount { get => counter; }
        public string Query
        {
            get
            {
                StringBuilder result = null;

                if (counter > 0)
                {
                    lock (queryBuilder)
                    {
                        result = new StringBuilder(queryBuilder.ToString());
                    }
                    result.Remove(queryBuilder.Length - 1, 1);
                    result.Append(';');
                }

                return result?.ToString();
            }
        }

        public delegate void InsertedArgs(int RowsCount);
        public event InsertedArgs Inserted;

        private readonly MySqlAdapter adapter;

        private StringBuilder queryBuilder = new StringBuilder();
        private StringBuilder paramBuilder = null;

        private int counter = 0;
        private int packageSize = 0;
        private int leftPartSize = 0;

        private Task syncTask;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter">MySqlAdapter адаптер</param>
        /// <param name="packageSize">Размер пакета</param>
        /// <param name="table">Таблица для вставки</param>
        /// <param name="insertIgnore">Вставка с игнорированием</param>
        /// <param name="columns">Столбцы</param>
        public InsertBuffer(MySqlAdapter adapter, int packageSize, string table, bool insertIgnore, params string[] columns)
        {
            this.packageSize = packageSize;
            this.adapter = adapter;

            queryBuilder = new StringBuilder($"INSERT {(!insertIgnore ? "INTO" : "IGNORE")} {table}(", 1000);

            for (int i = 0; i < columns.Length; i++)
            {
                queryBuilder.Append(columns[i]);
                queryBuilder.Append(',');
            }
            queryBuilder.Remove(queryBuilder.Length - 1, 1);
            queryBuilder.Append(") VALUES ");

            leftPartSize = queryBuilder.Length;
        }
        public InsertBuffer(MySqlAdapter adapter, TimeSpan syncInterval, int packageSize,  string table, bool insertIgnore, params string[] columns)
        {
            this.packageSize = packageSize;
            this.adapter = adapter;

            queryBuilder = new StringBuilder($"INSERT {(!insertIgnore ? "INTO" : "IGNORE")} {table}(", 1000);

            for (int i = 0; i < columns.Length; i++)
            {
                queryBuilder.Append(columns[i]);
                queryBuilder.Append(',');
            }
            queryBuilder.Remove(queryBuilder.Length - 1, 1);
            queryBuilder.Append(") VALUES ");

            leftPartSize = queryBuilder.Length;

            if (syncInterval > TimeSpan.Zero)
            {
                syncTask = Task.Run(() => 
                {
                    while (true)
                    {
                        try
                        {
                            Insert();
                        }
                        catch
                        {
                        }
                        finally
                        {
                            Thread.Sleep(syncInterval);
                        }
                    }
                });
            }
        }

        /// <summary>
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="values">Значения, через запятую.</param>
        public int Add(string values)
        {
            lock (queryBuilder)
            {
                Interlocked.Increment(ref counter);
                queryBuilder.Append("(" + values + "),");

                int result = 0;
                if (packageSize > 0 && counter >= packageSize)
                {
                    result = Insert();
                }

                return result;
            }
        }

        /// <summary>
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="values">Значения, через запятую.</param>
        public int Add(params string[] values)
        {
            lock (queryBuilder)
            {
                queryBuilder.Append('(');
                foreach (object item in values)
                {
                    queryBuilder.Append('\'');
                    queryBuilder.Append(item.ToString());
                    queryBuilder.Append("',");
                }
                queryBuilder.Remove(queryBuilder.Length - 1, 1);
                queryBuilder.Append("),");

                Interlocked.Increment(ref counter);
                int result = 0;
                if (packageSize > 0 && counter >= packageSize)
                {
                    result = Insert();
                }
                return result;
            }
        }

        /// <summary>
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="values">Значение полей. Апострофы из строк удаляются автоматически.</param>
        public int Add(params object[] values)
        {
            lock (queryBuilder)
            {
                queryBuilder.Append('(');
                foreach (object item in values)
                {
                    Type t = item.GetType();

                    queryBuilder.Append('\'');
                    switch (t.Name)
                    {
                        case "String":
                            queryBuilder.Append(item.ToString());
                            break;
                        case "Double":
                        case "Single":
                            queryBuilder.Append(item.ToString().Replace(',', '.'));
                            break;
                        case "DateTime":
                            queryBuilder.Append(((DateTime)item).ToString("s"));
                            break;
                        default:
                            queryBuilder.Append(item.ToString());
                            break;
                    }
                    queryBuilder.Append("',");
                }
                queryBuilder.Remove(queryBuilder.Length - 1, 1);
                queryBuilder.Append(')');
                queryBuilder.Append(',');

                Interlocked.Increment(ref counter);
                int result = -1;
                if (packageSize > 0 && counter >= packageSize)
                {
                    result = Insert();
                }
                return result;
            }
        }


        /// <summary>
        /// Вводит буфер в режим вставки одиночных значений.
        /// </summary>
        public void BeginAdd()
        {
            lock (paramBuilder)
            {
                if (paramBuilder == null)
                {
                    paramBuilder = new StringBuilder("(");
                }
                else
                {
                    throw new Exception("Параметры уже вводятся.");
                }
            }
        }

        /// <summary>
        /// Вставляет единичное значение.
        /// </summary>
        /// <param name="value"></param>
        public void AddSingle(string value)
        {
            lock (paramBuilder)
            {
                if (paramBuilder != null)
                {
                    paramBuilder.Append($"'{value}',");
                }
                else
                {
                    throw new Exception("Параметры еще не вводятся.");
                }
            }
        }

        /// <summary>
        /// Выводит буфер из режима одиночной вставки значений и добавляет значение в очередь.
        /// </summary>
        /// <param name="apply">Применить результат.</param>
        public void EndAdd(bool apply = true)
        {
            lock (paramBuilder)
            {
                if (paramBuilder != null)
                {
                    if (apply && paramBuilder.Length > 3)
                    {
                        paramBuilder.Remove(paramBuilder.Length - 1, 1);
                        paramBuilder.Append(')');
                        queryBuilder.Append(paramBuilder.ToString());
                        queryBuilder.Append(',');
                        paramBuilder = null;

                        Interlocked.Increment(ref counter);
                        int result = -1;
                        if (packageSize > 0 && counter >= packageSize)
                        {
                            result = Insert();
                        }
                    }
                }
                else
                {
                    throw new Exception("Параметры уже вводятся.");
                }
            }
        }

        /// <summary>
        /// Принудительно выполнить запрос и очистить очередь.
        /// </summary>
        public int Insert()
        {
            lock (queryBuilder)
            {
                int result = -1;
                if (counter > 0)
                {
                    queryBuilder.Remove(queryBuilder.Length - 1, 1);

                    //вставка
                    result = adapter.Execute(queryBuilder.ToString());

                    queryBuilder.Remove(leftPartSize, queryBuilder.Length - leftPartSize);
                    Interlocked.Exchange(ref counter, 0);
                }

                Inserted?.Invoke(result);
                return result;
            }
        }
    }
}
