using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace ITsoft.Extensions.MySql
{
    /// <summary>
    /// Буфер пакетного выполнения SQL запросов.
    /// </summary>
    public sealed class QueryBuffer : IDisposable
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
                        var query = _queryBuilder.ToString();
                        if (_useTransaction)
                        {
                            query = string.Concat(query, "COMMIT;");
                        }

                        return query;
                    }
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
        /// <param name="rowsCount">Количество задействованных строк.</param>
        public delegate void ExecutedArgs(int rowsCount);

        /// <summary>
        /// Вызывается поcле успешного выполнения запроса.
        /// </summary>
        public event ExecutedArgs AfterExecute;

        private readonly MySqlAdapter _adapter;
        private StringBuilder _queryBuilder = new StringBuilder();
        private int _counter = 0;
        private int _batchSize = 0;
        private bool _useTransaction = false;
        private List<int> _operationOffsets = new List<int>();

        private Timer _syncTimer;
        private DateTime _syncDateTime;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter">MySqlAdapter адаптер</param>
        /// <param name="batchSize">Размер пакета</param>
        /// <param name="useTransaction">Отсправить запрос как одну транзакцию.</param>
        public QueryBuffer(MySqlAdapter adapter, int batchSize, bool useTransaction = false)
        {
            this._batchSize = batchSize;
            this._adapter = adapter;
            this._useTransaction = useTransaction;

            if (useTransaction)
            {
                StartTransaction();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter">MySqlAdapter адаптер</param>
        /// <param name="syncInterval">Интервал по истечению которого произойдет выполнение буферизированных запросов.</param>
        /// <param name="batchSize">Размер пакета</param>
        /// <param name="useTransaction">Использовать транзакции для вставки буферизированных запросов.</param>
        public QueryBuffer(MySqlAdapter adapter, TimeSpan syncInterval, int batchSize, bool useTransaction = false)
        {
            _batchSize = batchSize;
            _adapter = adapter;
            _useTransaction = useTransaction;

            if (useTransaction)
            {
                StartTransaction();
            }

            _syncTimer = new Timer(async (object item) =>
            {
                try
                {
                    await ExecuteAsync();
                }
                catch
                {
                }
            }, null, syncInterval, syncInterval);
        }

        /// <summary>
        /// Начало транзакцию.
        /// </summary>
        public void StartTransaction()
        {
            _queryBuilder.AppendLine("START TRANSACTION;");
        }

        /// <summary>
        /// Конец транзакции.
        /// </summary>
        public void EndTransaction()
        {
            _queryBuilder.AppendLine("COMMIT;");
        }

        /// <summary>
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="query">Запрос для добавления в буфер.</param>
        public int Add(string query)
        {
            if (query?.Length > 0)
            {
                lock (_queryBuilder)
                {
                    Interlocked.Increment(ref _counter);
                    _operationOffsets.Add(_queryBuilder.Length);

                    var trimQuery = query.Trim('\r', '\n');
                    _queryBuilder.Append(trimQuery);

                    if (trimQuery[trimQuery.Length - 1] != ';')
                    {
                        _queryBuilder.Append(";");
                    }
                    _queryBuilder.AppendLine();

                    int result = 0;
                    if (_batchSize > 0 && _counter >= _batchSize)
                    {
                        result = Execute();
                    }

                    return result;
                }
            }

            return 0;
        }

        /// <summary>
        /// Отмена последних N операций вставки, которые еще не были выполнены.
        /// </summary>
        /// <param name="lastOperationsNumber"></param>
        public void Reject(int lastOperationsNumber)
        {
            lock (_queryBuilder)
            {
                var index = _operationOffsets.Count - lastOperationsNumber;
                var offset = _operationOffsets[index];
                var length = _queryBuilder.Length - offset;

                _queryBuilder.Remove(offset, length);
                _operationOffsets.RemoveRange(index, _operationOffsets.Count - index);
            }
        }

        /// <summary>
        /// Заменить значение.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="replacement"></param>
        public void Replace(Regex regex, string replacement)
        {
            lock (_queryBuilder)
            {
                var tmp = _queryBuilder.ToString();
                regex.Replace(tmp, replacement);

                _queryBuilder = new StringBuilder(tmp);
            }
        }

        /// <summary>
        /// Заменить значение.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="evalutor"></param>
        public void Replace(Regex regex, MatchEvaluator evalutor)
        {
            lock (_queryBuilder)
            {
                var tmp = _queryBuilder.ToString();
                regex.Replace(tmp, evalutor);

                _queryBuilder = new StringBuilder(tmp);
            }
        }


        /// <summary>
        /// Принудительно выполнить запрос и очистить очередь.
        /// </summary>
        public int Execute(int? timeOut = null, bool? retryOnError = null)
        {
            int result = -1;

            if (!SyncPaused)
            {
                string query = CreateQuery();
                if (query != null)
                {
                    //вставка
                    result = _adapter.Execute(query, timeOut, retryOnError);

                    SyncDateTime = DateTime.Now;
                    AfterExecute?.Invoke(result);
                }
            }

            return result;
        }

        /// <summary>
        /// Принудительно выполнить запрос и очистить очередь.
        /// </summary>
        public async Task<int> ExecuteAsync(int? timeOut = null, bool? retryOnError = null)
        {
            int result = -1;

            if (!SyncPaused)
            {
                string query = CreateQuery();
                if (query != null)
                {
                    //вставка
                    result = await _adapter.ExecuteAsync(query, timeOut, retryOnError);

                    SyncDateTime = DateTime.Now;
                    AfterExecute?.Invoke(result);
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
                    if (_useTransaction)
                    {
                        EndTransaction();
                    }

                    query = _queryBuilder.ToString();

                    _queryBuilder.Clear();
                    if (_useTransaction)
                    {
                        StartTransaction();
                    }

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
