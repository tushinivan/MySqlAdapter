
using ITsoft.Extensions.MySql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MySqlAdapterExaples
{
    class Program
    {
        static MySqlAdapter adapter = new MySqlAdapter("User Id=test; Password=test; Host=localhost;Character Set=utf8;");
        static void Main(string[] args)
        {
            adapter.Error += Adapter_Error;

            adapter.SelectReader("SELECT * FROM test.test_table WHERE id = 1", (row) =>
            {
                int id = Convert.ToInt32(row["id"]);
            });
        }

        private static void Adapter_Error(Exception ex, string query)
        {
            //Write Log
        }
    }
}
