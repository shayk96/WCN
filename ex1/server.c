#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 8080
#define BUFFER_SIZE 1024 * 1024
#define ACK "ACK"

void error(const char *msg) {
  perror(msg);
  exit(1);
}

int main() {
  int server_fd, new_socket;
  struct sockaddr_in address;
  int addrlen = sizeof(address);
  char buffer[BUFFER_SIZE] = {0};

  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      error("socket failed");
    }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(PORT);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
      error("bind failed");
    }

  if (listen(server_fd, 3) < 0) {
      error("listen failed");
    }

  if ((new_socket = accept(server_fd, (struct sockaddr* )&address, (socklen_t*)&addrlen)) < 0) {
      error("accept failed");
    }

  while (1) {
      long bytes_read = read(new_socket, buffer, BUFFER_SIZE);
      if (bytes_read <= 0) {
          break;
        }
      if (send(new_socket, ACK, strlen(ACK), 0) == -1) {
          error("send ACK failed");
        }
    }

  close(new_socket);
  close(server_fd);
  return 0;
}
