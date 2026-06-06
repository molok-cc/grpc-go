/*
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a http3 server for Greeter service.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/testdata"

	"github.com/quic-go/quic-go/http3"
)

type server struct {
	helloworld.UnimplementedGreeterServer
}

func (s *server) SayHello(_ context.Context, in *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	fmt.Printf("GRPC: Received SayHello(name=%q)\n", in.GetName())
	return &helloworld.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func main() {
	gs := grpc.NewServer()
	helloworld.RegisterGreeterServer(gs, &server{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("HTTP: Received request %s %s (Proto: %s)\n", r.Method, r.URL.Path, r.Proto)
		gs.ServeHTTP(w, r)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	cert, err := tls.LoadX509KeyPair(testdata.Path("x509/server1_cert.pem"), testdata.Path("x509/server1_key.pem"))
	if err != nil {
		log.Fatalf("failed to load key pair: %v", err)
	}

	addr := ":50051"
	ls, err := net.ListenPacket("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	s := &http3.Server{
		Handler: handler,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h3"},
		},
	}

	fmt.Printf("H3 Server listening on %s\n", addr)
	log.Fatal(s.Serve(ls))
}
