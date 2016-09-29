import sys.db.Postgresql;
import neko.net.Poll;

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

      trace('flush: ' + database.flush());
      trace('getResult: ' + database.getResult());
      trace('consumeInput: ' + database.consumeInput());
      var ret = database.sendQuery("SELECT * FROM Test ORDER BY random() LIMIT 10");
      trace('sendQuery:' + ret);
      trace('consumeInput: ' + database.consumeInput());
      var res = database.getResult();
      for (row in res)
        trace(row);
      trace('getResult: ' + database.getResult());

      var ret = database.sendQuery("SELECT * FROM Test ORDER BY random() LIMIT 10");
      trace('sendQuery:' + ret);

      var res = database.getResult();
      for (row in res)
        trace(row);

      // full cycle with long query
      trace('==================');
      trace('getResult: ' + database.getResult());
      trace('isBusy: ' + database.isBusy());
      var ret = database.sendQuery("SELECT * FROM Test WHERE ID = 4000!");
      trace('sendQuery: ' + ret);

      var p = new neko.net.Poll(1);
      var socks = [ database.getSocket() ];
      var t1 = Sys.time();
      while (true)
        {
//          trace('cycle');
          var ok = false;
          for (s in p.poll(socks, 0.01))
            {
              trace('poll once');
              ok = true;
            }

          if (ok)
            break;
        }
      trace('time: ' + (1000.0 * (Sys.time() - t1)));

      trace('consumeInput: ' + database.consumeInput());
      trace('isBusy: ' + database.isBusy());
      var t1 = Sys.time();
      var res = database.getResult();
      trace('time: ' + (1000.0 * (Sys.time() - t1)));
      for (row in res)
        trace(row);
      trace('getResult: ' + database.getResult());
    }


  public static function main()
    {
      var t = new Test();
      t.run();
    }
}
