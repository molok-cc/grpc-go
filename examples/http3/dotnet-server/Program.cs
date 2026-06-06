using Microsoft.AspNetCore.Server.Kestrel.Core;
using dotnet_server.Services;
using System.Net;

var builder = WebApplication.CreateBuilder(args);

// Configure Kestrel to use HTTP/3
builder.WebHost.ConfigureKestrel(options =>
{
    // Listen on port 50051 for HTTP/3 (UDP)
    options.Listen(IPAddress.Any, 50051, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http3;
        listenOptions.UseHttps(); // H3 requires HTTPS/TLS
    });
});

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<GreeterService>();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
