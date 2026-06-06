using Grpc.Core;
using Helloworld;

namespace dotnet_server.Services;

public class GreeterService : Greeter.GreeterBase
{
    private readonly ILogger<GreeterService> _logger;
    public GreeterService(ILogger<GreeterService> logger)
    {
        _logger = logger;
    }

    public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Received SayHello from {Name} via {Protocol}", request.Name, context.GetHttpContext().Request.Protocol);
        return Task.FromResult(new HelloReply
        {
            Message = "Hello " + request.Name + " from .NET H3"
        });
    }
}
