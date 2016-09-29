import sys.db.Postgresql;
import neko.net.Poll;

class StressTest
{
  public function new()
    {}


  public function run()
    {
      var conns = [];
      var socks = [];
      var states = [];
      var cnt = [];

      // async test
      for (i in 0...16)
        {
          // open connection
          var database: PostgresConnection = untyped Postgresql.open(
            "host=localhost port=5432 dbname=test" +
            " user=test password=123");
          database.setNonBlocking(true);
          conns.push(database);
          var s = database.getSocket();
          s.custom = i;
          socks.push(s);
          cnt.push(0);
          states.push('idle');
        }

      var p = new neko.net.Poll(conns.length);
      var t1 = Sys.time();
      var cntQuery = 0;

      // send queries
      for (id in 0...conns.length)
        {
          var c = conns[id];
          var ret = c.sendQuery("SELECT * FROM Test ORDER BY random() LIMIT 50");
          cntQuery++;
          cnt[id]++;
          states[id] = 'query';
          if (!ret)
            {
              trace('error sending query');
              return;
            }
        }

      while (true)
        {
          // poll for results
          for (s in p.poll(socks, 0.01))
            {
              var id: Int = s.custom;
              var c = conns[id];
//              trace(id + ' consumeInput: ' + c.consumeInput());
//              trace(id + ' isBusy: ' + c.isBusy());
              while (c.isBusy())
                c.consumeInput();
//              trace(id + ' isBusy: ' + c.isBusy());

              var res = c.getResult();
//              trace(res.length);
              c.getResult();
//              trace(id + ' getResult: ' + c.getResult());

              var ret = c.sendQuery("SELECT * FROM Test ORDER BY random() LIMIT 50");
              cnt[id]++;
              cntQuery++;
//              trace(id + ' sendQuery: ' + ret);
              states[id] = 'query';
              if (!ret)
                {
                  trace('error sending query');
                  return;
                }
            }

          if (cnt[0] % 100 == 0)
            {
              trace('array: ' + cnt);
              trace('total: ' + cntQuery);
            }
        }
      trace('time: ' + (1000.0 * (Sys.time() - t1)));
    }


  public static function main()
    {
      var t = new StressTest();
      t.run();
    }
}

