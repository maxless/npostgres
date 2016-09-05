import sys.db.Postgresql;

class Test
{
  public function new()
    {}


  public function run()
    {
      // open connection
      var database: PostgresConnection = untyped Postgresql.open(
        "host=localhost port=5432 dbname=test" +
        " user=test password=123");

      // basic select
      var res = database.request("SELECT * FROM Test ORDER BY random() LIMIT 10");
      for (row in res)
        trace(row);

      // set/check non-blocking
      trace(database.isNonBlocking());
      database.setNonBlocking(true);
      trace(database.isNonBlocking());

      // get socket
      var s = database.getSocket();
//      trace(s);
//      s.output.writeString('test');

      trace(database.flush());
      trace(database.getResult());
      var ret = database.sendQuery("SELECT * FROM Test ORDER BY random() LIMIT 10");
      trace(ret);
      var ret = database.sendQuery("SELECT * FROM Test ORDER BY random() LIMIT 10");
      trace(ret);

      var res = database.getResult();
      for (row in res)
        trace(row);
    }


  public static function main()
    {
      var t = new Test();
      t.run();
    }
}
