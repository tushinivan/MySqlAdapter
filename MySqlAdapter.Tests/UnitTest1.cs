﻿using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ITsoft.Extensions.MySql.Tests
{
    [TestClass]
    public class UnitTest1
    {
        MySqlAdapter adapter;


        [TestInitialize]
        public void Initialize()
        {
            adapter = new MySqlAdapter("User Id=test; Password=test; Host=localhost;Character Set=utf8;");
            adapter.Error += Adapter_Error;
        }

        private void Adapter_Error(Exception ex, string query)
        {
            
        }

        [TestMethod]
        public void CacheTest()
        {
            CacheOptions options = new CacheOptions()
            {
                CacheSize = 1000,
                SlidingExpiration = new TimeSpan(0, 1, 0)
            };
            var myDictionaryQuery = adapter.GetCacheQuery("my_dictionary", "SELECT id FROM test.my_dictionary WHERE code = {0}", options);

            while (true)
            {
                string code = "code_1";
                var table = myDictionaryQuery.Get(false, code);

                var value = myDictionaryQuery.GetScalar<int>(false, code);
            }
        }
    }
}
