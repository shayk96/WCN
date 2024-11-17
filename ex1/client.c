#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>

#define PORT 8080
#define MAX_MESSAGE_SIZE 1024 * 1024
#define NUM_MESSAGES 100
#define WARM_UP_CYCLES 100
#define IP_ADDRESS 1


void error(const char *msg) {
  perror(msg);
  exit(1);
}

void send_messages(int sock, int message_size) {
  char *buffer = (char *)malloc(message_size);
  memset(buffer, 'A', message_size);
  char ack[4];
  for (int i = 0; i < NUM_MESSAGES; i++) {
      if (send(sock, buffer, message_size, 0) == -1) {
          error("send failed");
        }
      if (recv(sock, ack, sizeof(ack), 0) == -1) {
          error("recv ACK failed");
        }
    }
  free(buffer);
}

void warm_up(int sock)
{
  for (int i = 0; i < WARM_UP_CYCLES; i++) {
      send_messages(sock, 1);
    }
}

void mesure_throughput(int sock)
{
  for (int message_size = 1; message_size <= MAX_MESSAGE_SIZE; message_size *= 2) {
      struct timespec start, end;
      clock_gettime(CLOCK_MONOTONIC, &start);
      send_messages(sock, message_size);
      clock_gettime(CLOCK_MONOTONIC, &end);
      double elapsed_time = end.tv_sec - start.tv_sec + (end.tv_nsec - start.tv_nsec) / 1e9;
      double throughput = ((message_size * NUM_MESSAGES) / (elapsed_time * 1024 * 1024))*8;
      printf("%d\t%.2f\tMb/s\n", message_size, throughput);
    }
}

int main(int argc, char const *argv[]) {
  if (argc != 2) {
      fprintf(stderr, "Usage: %s <server-ip>\n", argv[0]);
      exit(EXIT_FAILURE);
    }

  const char *server_ip = argv[IP_ADDRESS];
  int sock = 0;
  struct sockaddr_in serv_addr;

  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
      error("Socket creation error");
    }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);

  if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr) <= 0) {
      error("Invalid address/ Address not supported");
    }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
      error("Connection failed");
    }

  warm_up(sock);
  mesure_throughput(sock);
  close(sock);
  return 0;
}

