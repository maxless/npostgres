// batching queries vs one by one

import sys.db.Postgresql;
import neko.net.Poll;

class BatchTest
{
  public function new()
    {}


  static var MAX_QUERY = 10000;
  static var POLL_LIMIT = 0.001;
  public function run()
    {
      // open connection
      var c: PostgresConnection = untyped Postgresql.open(
        "host=localhost port=5432 dbname=test" +
        " user=test password=123");
      c.setNonBlocking(true);
      var socket = c.getSocket();
      var socks = [ socket ];
      var p = new neko.net.Poll(1);

      // one by one
      trace('one by one');
      var t1 = Sys.time();
      var cntQuery = 0;
      while (true)
        {
          var ret = c.sendQuery("SELECT * FROM Test ORDER BY random() LIMIT 50");
          cntQuery++;
          if (!ret)
            {
              trace('error sending query');
              return;
            }

          // flush
          while (c.flush() != 0)
            1;

          while (true)
            {
              var ok = false;

              // poll for results
              for (s in p.poll(socks, POLL_LIMIT))
                {
                  c.consumeInput();

                  while (c.isBusy())
                    1;

                  var res = c.getResult();
                  c.getResult();

                  ok = true;
                }

              if (ok)
                break;
            }

          if (cntQuery % 10000 == 0)
            trace('total: ' + cntQuery);

          if (cntQuery >= MAX_QUERY)
            break;
        }

      var tt = (1000.0 * (Sys.time() - t1));
      trace('time (one by one): ' + tt);

      var arr = [
        2, 3, 4, 5, 6, 7, 8, 9,
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 50, 100 ];
      for (x in arr)
        {
          var t = batch(x);
          trace('time (batch x' + x + '): ' + t + ', -' +
            (100.0 - 100.0 * t / tt) + '%');
        }
    }


  function batch(batch_size: Int): Float
    {
      // open connection
      var c: PostgresConnection = untyped Postgresql.open(
        "host=localhost port=5432 dbname=test" +
        " user=test password=123");
      c.setNonBlocking(true);
      var socket = c.getSocket();
      var socks = [ socket ];
      var p = new neko.net.Poll(1);

      // batches
      trace('batches x' + batch_size);
      var t1 = Sys.time();
      var cntQuery = 0;
      while (true)
        {
          var s = new StringBuf();
          for (i in 0...batch_size)
            {
              cntQuery++;
              s.add("SELECT * FROM Test ORDER BY random() LIMIT 50");
              s.add("; ");
            }

          var ret = c.sendQuery(s.toString());
          if (!ret)
            {
              trace('error sending query');
              Sys.exit(0);
            }

          // flush
          while (c.flush() != 0)
            1;

          while (true)
            {
              var ok = false;

              // poll for results
              for (s in p.poll(socks, POLL_LIMIT))
                {
                  c.consumeInput();

                  while (c.isBusy())
                    1;

                  for (i in 0...batch_size)
                    var res = c.getResult();
                  c.getResult();

                  ok = true;
                }

              if (ok)
                break;
            }

          if (cntQuery % 10000 == 0)
            trace('total: ' + cntQuery);

          if (cntQuery >= MAX_QUERY)
            break;
        }

      var t = 1000.0 * (Sys.time() - t1);
      return t;
    }


  public static function main()
    {
      var t = new BatchTest();
      t.run();
    }
}


