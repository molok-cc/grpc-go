using System.Net;
using System.Net.Quic;
using Grpc.Net.Client;
using Helloworld;

// Force 127.0.0.1 to avoid IPv6/localhost issues in containers
var address = args.Length > 0 ? args[0] : "https://127.0.0.1:50051";

Console.WriteLine($"QUIC Supported: {QuicConnection.IsSupported}");
Console.WriteLine($"Target Address: {address}");

var baseHandler = new SocketsHttpHandler
{
    SslOptions = new System.Net.Security.SslClientAuthenticationOptions
    {
        RemoteCertificateValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
    },
    EnableMultipleHttp2Connections = true
};

// This handler forces every single request to be HTTP/3 ONLY.
// If UDP fails, it will throw an exception instead of trying TCP.
var forceH3Handler = new ForceHttp3Handler(baseHandler);

using var httpClient = new HttpClient(forceH3Handler);

using var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
{
    HttpClient = httpClient,
    DisposeHttpClient = false
});

var client = new Greeter.GreeterClient(channel);

Console.WriteLine("Sending RPC call over forced HTTP/3 (UDP)...");
try
{
    var reply = await client.SayHelloAsync(new HelloRequest { Name = ".NET Client" });
    Console.WriteLine("Greeting: " + reply.Message);
}
catch (Exception ex)
{
    Console.WriteLine("Error: " + ex.Message);
    if (ex.InnerException != null)
    {
        Console.WriteLine("Inner Error: " + ex.InnerException.Message);
        if (ex.InnerException.InnerException != null)
        {
             Console.WriteLine("UDP/QUIC Error: " + ex.InnerException.InnerException.Message);
        }
    }
}

// Helper class to intercept gRPC requests and force version
class ForceHttp3Handler : DelegatingHandler
{
    public ForceHttp3Handler(HttpMessageHandler inner) : base(inner) { }
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        request.Version = HttpVersion.Version30;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        return base.SendAsync(request, cancellationToken);
    }
}
