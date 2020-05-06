using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ITsoft.Extensions.MySql
{
    /// <summary>
    /// Буфер пакетной вставки SQL запросов.
    /// </summary>
    public sealed class InsertBuffer : IDisposable
    {
        /// <summary>
        /// Количество запросов в очереди.
        /// </summary>
        public int Count { get => _counter; }

        /// <summary>
        /// SQL запрос.
        /// </summary>
        public string Query
        {
            get
            {
                StringBuilder result = null;

                if (_counter > 0)
                {
                    lock (_queryBuilder)
                    {
                        result = new StringBuilder(_queryBuilder.ToString());
                    }
                    result.Remove(_queryBuilder.Length - 1, 1);
                    result.Append(';');
                }

                return result?.ToString();
            }
        }

        /// <summary>
        /// Приостановить автоматическую синхронизацию.
        /// </summary>
        public bool SyncPaused { get; set; }

        /// <summary>
        /// Дата и время последней синхронизации.
        /// </summary>
        public DateTime SyncDateTime 
        {
            get 
            {
                if (_syncTimer != null)
                {
                    return _syncDateTime;
                }

                throw new NotImplementedException();
            }
            private set { _syncDateTime = value; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="rowsCount">Количество вставленных строк.</param>
        public delegate void InsertedArgs(int rowsCount);

        /// <summary>
        /// Вызывается поcле успешного выполнения запроса вставки.
        /// </summary>
        public event InsertedArgs AfterInsert;

        private readonly MySqlAdapter _adapter;

        private StringBuilder _queryBuilder = new StringBuilder();
        private StringBuilder _paramBuilder = null;

        private int _counter = 0;
        private int _batchSize = 0;
        private int _leftPartSize = 0;

        private Timer _syncTimer;
        private DateTime _syncDateTime;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter"><see cref="MySqlAdapter"/> адаптер</param>
        /// <param name="batchSize">Размер пакета.</param>
        /// <param name="table">Таблица для вставки/</param>
        /// <param name="insertIgnore">Вставка с игнорированием.</param>
        /// <param name="columns">Столбцы/</param>
        public InsertBuffer(MySqlAdapter adapter, int batchSize, string table, bool insertIgnore, params string[] columns)
        {
            _batchSize = batchSize;
            _adapter = adapter;

            _queryBuilder = new StringBuilder($"INSERT {(!insertIgnore ? "INTO" : "IGNORE")} {table}(", 1000);

            for (int i = 0; i < columns.Length; i++)
            {
                _queryBuilder.Append(columns[i]);
                _queryBuilder.Append(',');
            }
            _queryBuilder.Remove(_queryBuilder.Length - 1, 1);
            _queryBuilder.Append(") VALUES ");

            _leftPartSize = _queryBuilder.Length;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter"><see cref="MySqlAdapter"/> адаптер</param>
        /// <param name="batchSize">Размер пакета.</param>
        /// <param name="table">Таблица для вставки/</param>
        /// <param name="insertIgnore">Вставка с игнорированием.</param>
        /// <param name="syncInterval">Период с которым выполняется вставка.</param>
        /// <param name="columns">Столбцы/</param>
        public InsertBuffer(MySqlAdapter adapter, TimeSpan syncInterval, int batchSize, string table, bool insertIgnore, params string[] columns)
        {
            _batchSize = batchSize;
            _adapter = adapter;

            _queryBuilder = new StringBuilder($"INSERT {(!insertIgnore ? "INTO" : "IGNORE")} {table}(", 1000);

            for (int i = 0; i < columns.Length; i++)
            {
                _queryBuilder.Append(columns[i]);
                _queryBuilder.Append(',');
            }
            _queryBuilder.Remove(_queryBuilder.Length - 1, 1);
            _queryBuilder.Append(") VALUES ");

            _leftPartSize = _queryBuilder.Length;

            _syncTimer = new Timer(async (object item) =>
            {
                try
                {
                    await InsertAsync();
                }
                catch
                {
                }
            }, null, syncInterval, syncInterval);
        }

        /// <summary>
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="values">Значения, через запятую.</param>
        public int Add(string values)
        {
            lock (_queryBuilder)
            {
                Interlocked.Increment(ref _counter);
                _queryBuilder.Append("(" + values + "),");

                int result = 0;
                if (_batchSize > 0 && _counter >= _batchSize)
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
            lock (_queryBuilder)
            {
                _queryBuilder.Append('(');
                foreach (object item in values)
                {
                    _queryBuilder.Append('\'');
                    _queryBuilder.Append(item.ToString());
                    _queryBuilder.Append("',");
                }
                _queryBuilder.Remove(_queryBuilder.Length - 1, 1);
                _queryBuilder.Append("),");

                Interlocked.Increment(ref _counter);
                int result = 0;
                if (_batchSize > 0 && _counter >= _batchSize)
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
            lock (_queryBuilder)
            {
                _queryBuilder.Append('(');
                foreach (object item in values)
                {
                    Type t = item.GetType();

                    _queryBuilder.Append('\'');
                    switch (t.Name)
                    {
                        case "String":
                            _queryBuilder.Append(item.ToString());
                            break;
                        case "Double":
                        case "Single":
                            _queryBuilder.Append(item.ToString().Replace(',', '.'));
                            break;
                        case "DateTime":
                            _queryBuilder.Append(((DateTime)item).ToString("s"));
                            break;
                        default:
                            _queryBuilder.Append(item.ToString());
                            break;
                    }
                    _queryBuilder.Append("',");
                }
                _queryBuilder.Remove(_queryBuilder.Length - 1, 1);
                _queryBuilder.Append(')');
                _queryBuilder.Append(',');

                Interlocked.Increment(ref _counter);
                int result = -1;
                if (_batchSize > 0 && _counter >= _batchSize)
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
            lock (_paramBuilder)
            {
                if (_paramBuilder == null)
                {
                    _paramBuilder = new StringBuilder("(");
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
            lock (_paramBuilder)
            {
                if (_paramBuilder != null)
                {
                    _paramBuilder.Append($"'{value}',");
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
            lock (_paramBuilder)
            {
                if (_paramBuilder != null)
                {
                    if (apply && _paramBuilder.Length > 3)
                    {
                        _paramBuilder.Remove(_paramBuilder.Length - 1, 1);
                        _paramBuilder.Append(')');
                        _queryBuilder.Append(_paramBuilder.ToString());
                        _queryBuilder.Append(',');
                        _paramBuilder = null;

                        Interlocked.Increment(ref _counter);
                        int result = -1;
                        if (_batchSize > 0 && _counter >= _batchSize)
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
            int result = -1;

            if (!SyncPaused)
            {
                string query = CreateQuery();
                if (query != null)
                {
                    //вставка
                    result = _adapter.Execute(query);

                    SyncDateTime = DateTime.Now;
                    AfterInsert?.Invoke(result);
                }
            }
            

            return result;
        }

        /// <summary>
        /// Принудительно выполнить запрос и очистить очередь.
        /// </summary>
        public async Task<int> InsertAsync()
        {
            int result = -1;

            if (!SyncPaused)
            {
                string query = CreateQuery();
                if (query != null)
                {
                    //вставка
                    result = await _adapter.ExecuteAsync(query);

                    SyncDateTime = DateTime.Now;
                    AfterInsert?.Invoke(result);
                }
            }

            return result;
        }

        private string CreateQuery()
        {
            string query = null;

            if (_counter > 0)
            {
                lock (_queryBuilder)
                {
                    _queryBuilder.Remove(_queryBuilder.Length - 1, 1);
                    query = _queryBuilder.ToString();
                    _queryBuilder.Remove(_leftPartSize, _queryBuilder.Length - _leftPartSize);
                    Interlocked.Exchange(ref _counter, 0);
                }
            }

            return query;
        }

        /// <summary>
        /// Освобождает все ресурсы используемые данным экземпяром.
        /// </summary>
        public void Dispose()
        {
            _queryBuilder = null;
            if (_syncTimer != null)
            {
                _syncTimer.Dispose();
            }
        }
    }
}
