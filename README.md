# Test RPC Client/Server


Run Server: <code>java -cp &lt;classpath&gt; test.rpc.TestServer &lt;port&gt; [complex]</code>
<p>
If flag "complex" present -> start Server based on java.nio
else -> start single-threaded Server
</p>
Run Client: <code>java -cp &lt;classpath&gt; test.rpc.TestClient &lt;host&gt; &lt;port&gt;</code>
