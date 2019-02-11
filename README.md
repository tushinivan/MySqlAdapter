# MySqlAdapter
**MySqlAdapter** - easy helper to work with MySql.

Capability:
- Packing insert query
- Cahcing query
- Lopping query with error (e.t. deadlock table or transaction)
- Character escaping

Create MySqlAdapter:
```c#
MySqlAdapter adapter = new MySqlAdapter("User Id=test; Password=test; Host=localhost;Character Set=utf8;");

adapter.Error += Adapter_Error;
void Adapter_Error(Exception ex, string query)
{
  //Write to log
}
```

Using InsertBuffer to create bulk insert:
```c#
int packageSize = 3;
InsertBuffer buffer = new InsertBuffer(adapter, packageSize, "test.buffer_test", false, "id", "value");

for (int i = 0; i < totalCount; i++)
{
  buffer.Add(i, $"value_{i}");
}
```

Create caching query:
```c#
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
  
  //or get scalar value
  var value = myDictionaryQuery.GetScalar<int>(false, code);
}
```

