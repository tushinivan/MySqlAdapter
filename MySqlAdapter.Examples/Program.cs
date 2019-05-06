using Extensions.MySql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MySqlAdapterExaples
{
    class Program
    {
        static MySqlAdapter adapter = new MySqlAdapter("");
        static void Main(string[] args)
        {
            adapter.Error += Adapter_Error;
        }

        private static void Adapter_Error(Exception ex, string query)
        {
            //Write Log
        }
    }
}
