#include <arrow/api.h>

#include <arrow/flight/api.h>
#include <gflags/gflags.h>
#include <grpc++/grpc++.h>

#include <iostream>

#include "helloworld.pb.h"
#include "helloworld.grpc.pb.h"

// Flight service
class SimpleFlightServer : public arrow::flight::FlightServerBase {};

// gRPC service
class HelloWorldServiceImpl : public HelloWorldService::Service {
  grpc::Status SayHello(grpc::ServerContext* ctx, const HelloRequest* request,
                        HelloResponse* reply) override {
    const std::string& name = request->name();
    if (name.empty()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Must provide a name!");
    }
    reply->set_reply("Hello, " + name);
    return grpc::Status::OK;
  }
};

arrow::Status RunFlightGrpc() {
  std::unique_ptr<arrow::flight::FlightServerBase> server;
  server.reset(new SimpleFlightServer());

  arrow::flight::Location bind_location;
  ARROW_RETURN_NOT_OK(
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", 1234).Value(&bind_location));
  arrow::flight::FlightServerOptions options(bind_location);

  HelloWorldServiceImpl grpc_service;
  int extra_port = 0;

  options.builder_hook = [&](void* raw_builder) {
    auto* builder = reinterpret_cast<grpc::ServerBuilder*>(raw_builder);
    builder->AddListeningPort("0.0.0.0:0", grpc::InsecureServerCredentials(),
                              &extra_port);
    builder->RegisterService(&grpc_service);
  };
  ARROW_RETURN_NOT_OK(server->Init(options));
  // std::cout << "Listening on ports " << 1234 << " and " << extra_port << std::endl;
  // ARROW_RETURN_NOT_OK(server->SetShutdownOnSignals({SIGTERM}));
  // ARROW_RETURN_NOT_OK(server->Serve());

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto status = RunFlightGrpc();
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
