FROM mcr.microsoft.com/dotnet/sdk:10.0

WORKDIR /server

COPY packages-microsoft-prod.deb .

RUN dpkg -i packages-microsoft-prod.deb && \
    apt-get update && \
    apt-get install -y libmsquic

COPY . .

RUN dotnet build -c Release
