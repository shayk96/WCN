#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <sys/param.h>
#include <stdbool.h>

#include <infiniband/verbs.h>

#pragma clang diagnostic push
#pragma ide diagnostic ignored "UnreachableCode"

#define WC_BATCH 1
#define NUM_PROCESSES 4
#define ITERATIONS 6
#define START_GATHER 3
#define START_NODE 0

/// @brief Work Request IDs for RDMA operations.
enum {
    PINGPONG_RECV_WRID = 1,  ///< WRID for receive operations.
    PINGPONG_SEND_WRID = 2,  ///< WRID for send operations.
};

/**
 * @enum OPERATION
 * @brief Enumeration representing different arithmetic operations.
 */
enum OPERATION {
    ADDITION = 0,        ///< Addition operation.
    MULTIPLICATION = 1,  ///< Multiplication operation.
} typedef OPERATION;

/**
 * @enum DATA_TYPE
 * @brief Enumeration representing different data types.
 */
enum DATA_TYPE {
    INT = 0,   ///< Integer data type.
    FLOAT = 1, ///< Floating-point data type.
} typedef DATA_TYPE;

/// @brief Stores the system's memory page size.
static int page_size;

/**
 * @struct shared_context
 * @brief Represents an RDMA shared context with necessary resources.
 */
struct shared_context {
    struct ibv_context *context;         ///< Infiniband verbs context.
    struct ibv_comp_channel *channel;    ///< Completion event channel.
};

/**
 * @struct pingpong_context
 * @brief Represents the RDMA context for a ping-pong test.
 */
struct pingpong_context {
    struct ibv_context *context;      ///< RDMA device context.
    struct ibv_comp_channel *channel; ///< Completion event channel.
    struct ibv_pd *pd;                ///< Protection domain.
    struct ibv_mr *mr;                ///< Memory region for RDMA operations.
    struct ibv_cq *cq;                ///< Completion queue.
    struct ibv_qp *qp;                ///< Queue pair.
    void *buf;                         ///< Buffer for RDMA operations.
    int size;                          ///< Size of the buffer.
    int rx_depth;                      ///< Receive queue depth.
    int routs;                         ///< Number of outstanding receives.
    struct ibv_port_attr portinfo;     ///< Attributes of the RDMA device port.
};

/**
 * @struct pingpong_dest
 * @brief Represents a remote endpoint in the RDMA ping-pong test.
 */
struct pingpong_dest {
    int lid;       ///< Local Identifier (LID) of the remote node.
    int qpn;       ///< Queue Pair Number (QPN).
    int psn;       ///< Packet Sequence Number (PSN).
    union ibv_gid gid; ///< Global Identifier (GID) for RoCE support.
};

/**
 * @brief Converts an integer MTU size to the corresponding `ibv_mtu` enumeration.
 *
 * @param mtu The MTU size in bytes (256, 512, 1024, 2048, or 4096).
 * @return The corresponding `ibv_mtu` enumeration value or -1 if invalid.
 */
enum ibv_mtu pp_mtu_to_enum(int mtu)
{
  switch (mtu) {
      case 256:  return IBV_MTU_256;
      case 512:  return IBV_MTU_512;
      case 1024: return IBV_MTU_1024;
      case 2048: return IBV_MTU_2048;
      case 4096: return IBV_MTU_4096;
      default:   return -1; ///< Return invalid value for unsupported MTU.
    }
}

/**
 * @brief Retrieves the Local Identifier (LID) of the given port.
 *
 * @param context Pointer to the RDMA device context.
 * @param port The port number to query.
 * @return The LID of the given port, or 0 if the query fails.
 */
uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
  struct ibv_port_attr attr;

  if (ibv_query_port(context, port, &attr))
    return 0;

  return attr.lid;
}

/**
 * @brief Retrieves the port attributes for a given RDMA device port.
 *
 * @param context Pointer to the RDMA device context.
 * @param port The port number to query.
 * @param attr Pointer to an `ibv_port_attr` structure to store the retrieved information.
 * @return 0 on success, nonzero on failure.
 */
int pp_get_port_info(struct ibv_context *context, int port, struct ibv_port_attr *attr)
{
  return ibv_query_port(context, port, attr);
}

/**
 * @brief Converts a wire-format GID string to an `ibv_gid` structure.
 *
 * @param wgid The wire-format GID string.
 * @param gid Pointer to the destination `ibv_gid` structure.
 */
void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
  char tmp[9];
  uint32_t v32;
  int i;

  for (tmp[8] = 0, i = 0; i < 4; ++i) {
      memcpy(tmp, wgid + i * 8, 8);
      sscanf(tmp, "%x", &v32);
      *(uint32_t *) (&gid->raw[i * 4]) = ntohl(v32);
    }
}

/**
 * @brief Converts an `ibv_gid` structure to a wire-format GID string.
 *
 * @param gid Pointer to the `ibv_gid` structure.
 * @param wgid The output wire-format GID string.
 */
void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
  for (int i = 0; i < 4; ++i)
    sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *) (gid->raw + i * 4)));
}

/**
 * @brief Establishes a connection between two RDMA queue pairs.
 *
 * @param ctx Pointer to the local `pingpong_context`.
 * @param port The local port number.
 * @param my_psn The packet sequence number (PSN) for the local side.
 * @param mtu The maximum transmission unit (MTU) size.
 * @param sl The service level (SL) for the connection.
 * @param dest Pointer to the remote `pingpong_dest` containing its connection information.
 * @param sgid_idx Source GID index for Global Routing Header (GRH), if applicable.
 * @return 0 on success, 1 on failure.
 */
static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn, enum ibv_mtu mtu, int sl, struct pingpong_dest *dest, int sgid_idx)
{
  struct ibv_qp_attr attr = {
      .qp_state             = IBV_QPS_RTR,
      .path_mtu             = mtu,
      .dest_qp_num          = dest->qpn,
      .rq_psn               = dest->psn,
      .max_dest_rd_atomic   = 1,
      .min_rnr_timer        = 12,
      .ah_attr              = {
          .is_global        = 0,
          .dlid            = dest->lid,
          .sl              = sl,
          .src_path_bits   = 0,
          .port_num        = port
      }
  };

  // If the remote destination uses a global identifier (GID)
  if (dest->gid.global.interface_id) {
      attr.ah_attr.is_global = 1;
      attr.ah_attr.grh.hop_limit = 1;
      attr.ah_attr.grh.dgid = dest->gid;
      attr.ah_attr.grh.sgid_index = sgid_idx;
    }

  // Modify the queue pair (QP) state to Ready to Receive (RTR)
  if (ibv_modify_qp(ctx->qp, &attr,
                    IBV_QP_STATE |
                    IBV_QP_AV |
                    IBV_QP_PATH_MTU |
                    IBV_QP_DEST_QPN |
                    IBV_QP_RQ_PSN |
                    IBV_QP_MAX_DEST_RD_ATOMIC |
                    IBV_QP_MIN_RNR_TIMER)) {
      fprintf(stderr, "Failed to modify QP to RTR\n");
      return 1;
    }

  return 0;
}

/**
 * @brief Exchanges RDMA connection details with a server.
 *
 * This function connects to the RDMA server at the specified hostname and port,
 * sends the local connection information, and receives the remote destination's details.
 *
 * @param servername The hostname or IP address of the RDMA server.
 * @param port The TCP port to connect to on the server.
 * @param my_dest Pointer to the local `pingpong_dest` structure containing RDMA connection details.
 * @return Pointer to a newly allocated `pingpong_dest` structure containing the remote connection details,
 *         or `NULL` on failure.
 */
static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port, const struct pingpong_dest *my_dest)
{
  struct addrinfo *res, *t;
  struct addrinfo hints = {
      .ai_family   = AF_INET,
      .ai_socktype = SOCK_STREAM
  };
  char *service;
  char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
  int n;
  int sockfd = -1;
  struct pingpong_dest *rem_dest = NULL;
  char gid[33];

  if (asprintf(&service, "%d", port) < 0)
    return NULL;

  n = getaddrinfo(servername, service, &hints, &res);
  if (n < 0) {
      fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
      free(service);
      return NULL;
    }

  for (t = res; t; t = t->ai_next) {
      sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
      if (sockfd >= 0) {
          if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
            break;
          close(sockfd);
          sockfd = -1;
        }
    }

  freeaddrinfo(res);
  free(service);

  if (sockfd < 0) {
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
      return NULL;
    }

  gid_to_wire_gid(&my_dest->gid, gid);
  sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
  if (write(sockfd, msg, sizeof msg) != sizeof msg) {
      fprintf(stderr, "Couldn't send local address\n");
      goto out;
    }

  if (read(sockfd, msg, sizeof msg) != sizeof msg) {
      perror("client read");
      fprintf(stderr, "Couldn't read remote address\n");
      goto out;
    }

  write(sockfd, "done", sizeof "done");

  rem_dest = malloc(sizeof *rem_dest);
  if (!rem_dest)
    goto out;

  sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
  wire_gid_to_gid(gid, &rem_dest->gid);

  out:
  close(sockfd);
  return rem_dest;
}

/**
 * @brief Handles the server-side RDMA connection exchange.
 *
 * The server listens on the given port, accepts a connection from a client,
 * receives the client's RDMA connection details, and sends back its own details.
 * The function also connects the local QP to the remote peer.
 *
 * @param ctx Pointer to the local `pingpong_context` structure.
 * @param ib_port The RDMA device port to use.
 * @param mtu The maximum transmission unit (MTU) size.
 * @param port The TCP port on which the server listens for connections.
 * @param sl The service level (SL) for the RDMA connection.
 * @param my_dest Pointer to the local `pingpong_dest` structure containing connection details.
 * @param sgid_idx The GID index to use for the RDMA connection.
 * @return Pointer to a newly allocated `pingpong_dest` structure containing the remote connection details,
 *         or `NULL` on failure.
 */
static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx, int ib_port, enum ibv_mtu mtu, int port, int sl, const struct pingpong_dest *my_dest, int sgid_idx)
{
  struct addrinfo *res, *t;
  struct addrinfo hints = {
      .ai_flags    = AI_PASSIVE,
      .ai_family   = AF_INET,
      .ai_socktype = SOCK_STREAM
  };
  char *service;
  char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
  int n;
  int sockfd = -1, connfd;
  struct pingpong_dest *rem_dest = NULL;
  char gid[33];

  if (asprintf(&service, "%d", port) < 0)
    return NULL;

  n = getaddrinfo(NULL, service, &hints, &res);
  if (n < 0) {
      fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
      free(service);
      return NULL;
    }

  for (t = res; t; t = t->ai_next) {
      sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
      if (sockfd >= 0) {
          n = 1;
          setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
          if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
            break;
          close(sockfd);
          sockfd = -1;
        }
    }

  freeaddrinfo(res);
  free(service);

  if (sockfd < 0) {
      fprintf(stderr, "Couldn't listen to port %d\n", port);
      return NULL;
    }

  listen(sockfd, 1);
  connfd = accept(sockfd, NULL, 0);
  close(sockfd);
  if (connfd < 0) {
      fprintf(stderr, "accept() failed\n");
      return NULL;
    }

  n = read(connfd, msg, sizeof msg);
  if (n != sizeof msg) {
      perror("server read");
      fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int)sizeof msg);
      goto out;
    }

  rem_dest = malloc(sizeof *rem_dest);
  if (!rem_dest)
    goto out;

  sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
  wire_gid_to_gid(gid, &rem_dest->gid);

  if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx)) {
      fprintf(stderr, "Couldn't connect to remote QP\n");
      free(rem_dest);
      rem_dest = NULL;
      goto out;
    }

  gid_to_wire_gid(&my_dest->gid, gid);
  sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
  if (write(connfd, msg, sizeof msg) != sizeof msg) {
      fprintf(stderr, "Couldn't send local address\n");
      free(rem_dest);
      rem_dest = NULL;
      goto out;
    }

  read(connfd, msg, sizeof msg);

  out:
  close(connfd);
  return rem_dest;
}


static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,int rx_depth, int tx_depth, int port, int use_event, int is_server, struct shared_context* shared_ctx)
{
  struct pingpong_context *ctx;
  ctx = calloc(1, sizeof *ctx);
  if (!ctx)
    return NULL;

  ctx->context = shared_ctx->context;
  ctx->channel = shared_ctx->channel;

  ctx->size = size;
  ctx->rx_depth = rx_depth;
  ctx->routs = rx_depth;

  ctx->buf = malloc(roundup(size, page_size));
  if (!ctx->buf) {
      fprintf(stderr, "Couldn't allocate work buf.\n");
      return NULL;
    }

  ctx->pd = ibv_alloc_pd(ctx->context);
  if (!ctx->pd) {
      fprintf(stderr, "Couldn't allocate PD\n");
      return NULL;
    }
  ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE);
  if (!ctx->mr) {
      fprintf(stderr, "Couldn't register MR\n");
      return NULL;
    }

  ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                          ctx->channel, 0);
  if (!ctx->cq) {
      fprintf(stderr, "Couldn't create CQ\n");
      return NULL;
    }

  {
    struct ibv_qp_init_attr attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap     = {
            .max_send_wr  = tx_depth,
            .max_recv_wr  = rx_depth,
            .max_send_sge = 1,
            .max_recv_sge = 1
        },
        .qp_type = IBV_QPT_RC
    };

    ctx->qp = ibv_create_qp(ctx->pd, &attr);
    if (!ctx->qp)  {
        fprintf(stderr, "Couldn't create QP\n");
        return NULL;
      }
  }

  {
    struct ibv_qp_attr attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = port,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE
    };

    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE              |
                      IBV_QP_PKEY_INDEX         |
                      IBV_QP_PORT               |
                      IBV_QP_ACCESS_FLAGS)) {
        fprintf(stderr, "Failed to modify QP to INIT\n");
        return NULL;
      }
  }

  return ctx;
}

int pp_close_ctx(struct pingpong_context *ctx)
{
  if (ibv_destroy_qp(ctx->qp)) {
      fprintf(stderr, "Couldn't destroy QP\n");
      return 1;
    }

  if (ibv_destroy_cq(ctx->cq)) {
      fprintf(stderr, "Couldn't destroy CQ\n");
      return 1;
    }

  if (ibv_dereg_mr(ctx->mr)) {
      fprintf(stderr, "Couldn't deregister MR\n");
      return 1;
    }

  if (ibv_dealloc_pd(ctx->pd)) {
      fprintf(stderr, "Couldn't deallocate PD\n");
      return 1;
    }

  if (ctx->channel) {
      if (ibv_destroy_comp_channel(ctx->channel)) {
          fprintf(stderr, "Couldn't destroy completion channel\n");
          return 1;
        }
    }

  if (ibv_close_device(ctx->context)) {
      fprintf(stderr, "Couldn't release context\n");
      return 1;
    }

  free(ctx->buf);
  free(ctx);

  return 0;
}

static int pp_post_recv(struct pingpong_context *ctx, int n)
{
  struct ibv_sge list = {
      .addr    = (uintptr_t) ctx->buf,
      .length = ctx->size,
      .lkey    = ctx->mr->lkey
  };
  struct ibv_recv_wr wr = {
      .wr_id        = PINGPONG_RECV_WRID,
      .sg_list    = &list,
      .num_sge    = 1,
      .next       = NULL
  };
  struct ibv_recv_wr *bad_wr;

  return ibv_post_recv(ctx->qp, &wr, &bad_wr);
}

static int pp_post_send(struct pingpong_context *ctx)
{
  struct ibv_sge list = {
      .addr    = (uint64_t) ctx->buf,
      .length = ctx->size,
      .lkey    = ctx->mr->lkey
  };

  struct ibv_send_wr *bad_wr, wr = {
      .wr_id        = PINGPONG_SEND_WRID,
      .sg_list    = &list,
      .num_sge    = 1,
      .opcode     = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
      .next       = NULL
  };

  return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

int pp_wait_completions(struct pingpong_context *ctx, int iters)
{
  struct ibv_wc wc[WC_BATCH];
  int ne, i;

      do {
          ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc);
          if (ne < 0) {
              fprintf(stderr, "poll CQ failed %d\n", ne);
              return 1;
            }

        } while (ne < 1);

  if (wc[0].status != IBV_WC_SUCCESS) {
      fprintf(stderr, "Failed status %s (%d) for wr_id %d\n", ibv_wc_status_str(wc[0].status), wc[0].status, (int)wc[0].wr_id);
      return -1;
    }

  return 0;
}

static void usage(const char *argv0)
{
  printf("Usage:\n");
  printf("  %s            start a server and wait for connection\n", argv0);
  printf("  %s <host>     connect to server at <host>\n", argv0);
  printf("\n");
  printf("Options:\n");
  printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
  printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
  printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
  printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
  printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
  printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
  printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
  printf("  -l, --sl=<sl>          service level value\n");
  printf("  -e, --events           sleep on CQ events (default poll)\n");
  printf("  -g, --gid-idx=<gid index> local port gid index\n");
  printf("  -x,  len               the vector length \n");
  printf("  -v, vec_arr            the vector\n");
  printf("  -k, rank               node rank\n");
}


struct pingpong_dest get_dest (struct pingpong_dest *my_dest, struct pingpong_dest *rem_dest, const char *servername, int port, int ib_port, enum ibv_mtu *mtu, int sl, int gidx, char *gid, int rank, struct pingpong_context *ctx, int client)
{
  if (client == true)
    rem_dest = pp_client_exch_dest(servername, port, my_dest);
  else
    rem_dest = pp_server_exch_dest(ctx, ib_port, (*mtu), port, sl, my_dest, gidx);

  if (!rem_dest)
    exit(1);

  inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
  printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

  if (client == true)
    if (pp_connect_ctx(ctx, ib_port, (*my_dest).psn, (*mtu), sl, rem_dest, gidx))
      exit(1);

  return (*my_dest);
}


void adjust_ctx (struct pingpong_dest *my_dest, int ib_port, int use_event, int gidx, char *gid, struct pingpong_context *ctx)
{
  if (use_event)
    if (ibv_req_notify_cq(ctx->cq, 0)) {
        fprintf(stderr, "Couldn't request CQ notification\n");
        exit(1);
      }

  if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
      fprintf(stderr, "Couldn't get port info\n");
      exit(1);
    }

  (*my_dest).lid = ctx->portinfo.lid;
  if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !(*my_dest).lid) {
      fprintf(stderr, "Couldn't get local LID\n");
      exit(1);
    }

  if (gidx >= 0) {
      if (ibv_query_gid(ctx->context, ib_port, gidx, &(*my_dest).gid)) {
          fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
          exit(1);
        }
    } else
    memset(&(*my_dest).gid, 0, sizeof (*my_dest).gid);

  (*my_dest).qpn = ctx->qp->qp_num;
  (*my_dest).psn = lrand48() & 0xffffff;
  inet_ntop(AF_INET6, &(*my_dest).gid, gid, sizeof gid);
  printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n", (*my_dest).lid, (*my_dest).qpn, (*my_dest).psn, gid);
}

//void send_vec (struct pingpong_context *out_ctx, int *vec_arr, int tx_depth, int len)
//{
//  memcpy(out_ctx->buf, vec_arr, len * sizeof (int));
//  if (pp_post_send(out_ctx)) {
//      fprintf(stderr, "Client couldn't post send\n");
//      exit(1);
//    }
//
//  pp_wait_completions(out_ctx, tx_depth);
//  printf("Client Done.\n");
//}
//
//void receive_vec(struct pingpong_context *in_ctx, int iters, int len)
//{
//  if (pp_post_recv(in_ctx, 1)) {
//      fprintf(stderr, "Server couldn't post receive\n");
//      exit(1);
//    }
//
//  int *vec_arr = in_ctx->buf;
//  pp_wait_completions(in_ctx, iters);
//  for (int i = 0; i < len; i++){
//      printf("%d,", vec_arr[i]);
//    }
//  printf("\n");
//  printf("Server Done.\n");
//}


void copy_pair (const struct pingpong_context *out_ctx, const void *vec_arr, int index, int i, DATA_TYPE dt){

  int size = dt == INT ? sizeof(int) : sizeof(float);
  memcpy(out_ctx->buf + (size * (2 * i)), &vec_arr[index + i], size);
  int tmp_idx = index + i;
  memcpy(out_ctx->buf + (size * ((2 * i) + 1)) , &tmp_idx, size);
}


void send_idx_pair(struct pingpong_context *out_ctx, void *vec_arr, int tx_depth, int index, int seg, DATA_TYPE dt)
{
  for(int i = 0 ; i < seg; i++){
      copy_pair (out_ctx, vec_arr, index, i, dt);
    }

  if (pp_post_send(out_ctx)) {
      fprintf(stderr, "Client couldn't post send\n");
      exit(1);
    }

  pp_wait_completions(out_ctx, tx_depth);
  printf("Client Done.\n");
}

void reduce (int *vec_arr, int final, int index, int item, OPERATION op){
  switch (op)
    {
      case ADDITION:
        vec_arr[index] = (vec_arr[index] * final) + item;

      break;

      case MULTIPLICATION:
        vec_arr[index] = final == 1 ? (vec_arr[index] * item) : item;
      break;

      default:
        break;
    }
  printf("item: %d, index: %d, final: %d\n", item, index, final);

}


void receive_idx_pair (struct pingpong_context *in_ctx, int iters, void* vec_arr, int len, int final, int seg, OPERATION op, DATA_TYPE dt)
{
  float* tmp_arr = (float*) vec_arr;

  if (dt == INT)
    {
      int* tmp_arr = (int *) vec_arr;
    }

  if (pp_post_recv(in_ctx, iters)) {
      fprintf(stderr, "Server couldn't post receive\n");
      exit(1);
    }

  pp_wait_completions(in_ctx, iters);
  int *received_arr = in_ctx->buf;

  for(int i = 0 ; i < seg; i++){
    int index = received_arr[(i*2)+1];
    int item = received_arr[i*2];
      reduce(vec_arr, final, index, item, op);
      printf("item: %d, index: %d\n", item, index);
    }
  for (int i = 0; i < len; i++){
      dt == INT ? printf("%d,", tmp_arr[i]): printf("%f,", tmp_arr[i]);
    }
  printf("\n");
  printf("Server Done.\n");
}

void pg_all_reduce (int tx_depth, int iters, int rank, void *vec_arr, int len, int tmp_len, struct pingpong_context *in_ctx, struct pingpong_context *out_ctx, OPERATION op ,DATA_TYPE dt)
{
  int seg = tmp_len / NUM_PROCESSES;
  int final = 1;
  for (int i = 0; i < ITERATIONS; i++){
      if ( i == START_GATHER ) {
        final = 0;
      }
      int index = ((rank - i + tmp_len * 2) % NUM_PROCESSES) * seg;
      if (rank == START_NODE){
          send_idx_pair (out_ctx, vec_arr, tx_depth, index, seg, dt);
          receive_idx_pair (in_ctx, iters, vec_arr, len, final, seg, op, dt);
        } else {
          receive_idx_pair (in_ctx, iters, vec_arr, len, final, seg, op, dt);
          send_idx_pair (out_ctx, vec_arr, tx_depth, index, seg, dt);
        }
    }
}


int main(int argc, char *argv[])
{
  struct ibv_device **dev_list;
  struct ibv_device *ib_dev;
  struct pingpong_context *ctx;
  struct pingpong_dest in_my_dest;
  struct pingpong_dest out_my_dest;
  struct pingpong_dest *in_rem_dest;
  struct pingpong_dest *out_rem_dest;
  char *ib_devname = NULL;
  char *servername;
  int port = 3659;
  int ib_port = 1;
  enum ibv_mtu mtu = IBV_MTU_2048;
  int rx_depth = 100;
  int tx_depth = 1;
  int iters = 1;
  int use_event = 0;
  int size = 32;
  int sl = 0;
  int gidx = -1;
  char gid[33];
  int rank;
  int *vec_arr;
  int len;
  int tmp_len;
  OPERATION op;
  DATA_TYPE dt;

  srand48(getpid() * time(NULL));

  while (1) {
      int c;

      static struct option long_options[] = {
          {.name = "port", .has_arg = 1, .val = 'p'},
          {.name = "ib-dev", .has_arg = 1, .val = 'd'},
          {.name = "ib-port", .has_arg = 1, .val = 'i'},
          {.name = "size", .has_arg = 1, .val = 's'},
          {.name = "mtu", .has_arg = 1, .val = 'm'},
          {.name = "rx-depth", .has_arg = 1, .val = 'r'},
          {.name = "iters", .has_arg = 1, .val = 'n'},
          {.name = "sl", .has_arg = 1, .val = 'l'},
          {.name = "events", .has_arg = 0, .val = 'e'},
          {.name = "gid-idx", .has_arg = 1, .val = 'g'},
          {.name = "rank", .has_arg = 1, .val = 'k'},
          {.name = "vec", .has_arg = 1, .val = 'v'},
          {.name = "len", .has_arg = 1, .val = 'x'},
          {.name = "op", .has_arg = 1, .val = 'o'},
          {.name = "dt", .has_arg = 1, .val = 't'},
          {0}
      };

      c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:e:g:k:v:x:o:t:", long_options, NULL);
      if (c == -1)
        break;

      switch (c) {
          case 'p':
            port = strtol(optarg, NULL, 0);
          if (port < 0 || port > 65535) {
              usage(argv[0]);
              return 1;
            }
          break;

          case 'd':
            ib_devname = strdup(optarg);
          break;

          case 'i':
            ib_port = strtol(optarg, NULL, 0);
          if (ib_port < 0) {
              usage(argv[0]);
              return 1;
            }
          break;

          case 's':
            size = strtol(optarg, NULL, 0);
          break;

          case 'm':
            mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
          if (mtu < 0) {
              usage(argv[0]);
              return 1;
            }
          break;

          case 'r':
            rx_depth = strtol(optarg, NULL, 0);
          break;

          case 'n':
            iters = strtol(optarg, NULL, 0);
          break;

          case 'l':
            sl = strtol(optarg, NULL, 0);
          break;

          case 'e':
            ++use_event;
          break;

          case 'g':
            gidx = strtol(optarg, NULL, 0);
          break;

          case 'k':
            rank = strtol(optarg, NULL, 0);
          break;

          case 'x':
            len = strtol(optarg, NULL, 0);
          tmp_len = len % NUM_PROCESSES == 0 ? len : len + (NUM_PROCESSES - len % NUM_PROCESSES);
          if (dt == INT)
            vec_arr = calloc(tmp_len, sizeof(int));
          else
            vec_arr = calloc(tmp_len, sizeof(float));
          break;

          case 'v':
            vec_arr[0] = dt == INT ? atoi(strtok(optarg, ",")) : atof(strtok(optarg, ","));
          for (int i = 1; i < len; i++){
              vec_arr[i] = dt == INT ? atoi(strtok(NULL, ",")) : atof(strtok(NULL, ","));
            }
          break;

          case 'o':
            op = strtol(optarg, NULL, 0) == 0 ? ADDITION : MULTIPLICATION;
          break;

          case 't':
            dt = strtol(optarg, NULL, 0) == 0 ? INT : FLOAT;
            break;

          default:
            usage(argv[0]);
          return 1;
        }
    }

  for (int i = 0; i < len; i++){
      printf("%d,", vec_arr[i]);
    }
  printf("\n");

  if (optind == argc - 1)
    servername = strdup(argv[optind]);
  else if (optind < argc) {
      usage(argv[0]);
      return 1;
    }

  page_size = sysconf(_SC_PAGESIZE);

  dev_list = ibv_get_device_list(NULL);
  if (!dev_list) {
      perror("Failed to get IB devices list");
      return 1;
    }

  if (!ib_devname) {
      ib_dev = *dev_list;
      if (!ib_dev) {
          fprintf(stderr, "No IB devices found\n");
          return 1;
        }
    } else {
      int i;
      for (i = 0; dev_list[i]; ++i)
        if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
          break;
      ib_dev = dev_list[i];
      if (!ib_dev) {
          fprintf(stderr, "IB device %s not found\n", ib_devname);
          return 1;
        }
    }

  struct shared_context *shared_ctx;
  shared_ctx = calloc(1, sizeof(shared_ctx));
  shared_ctx->context = ibv_open_device(ib_dev);
  if (!shared_ctx->context) {
      fprintf(stderr, "Couldn't get context for %s\n", ibv_get_device_name(ib_dev));
      return 1;
    }

  if (use_event) {
      shared_ctx->channel = ibv_create_comp_channel(shared_ctx->context);
      if (!shared_ctx->channel) {
          fprintf(stderr, "Couldn't create completion channel\n");
          return 1;
        }
    } else
    shared_ctx->channel = NULL;

  struct pingpong_context *in_ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername, shared_ctx);
  struct pingpong_context *out_ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername, shared_ctx);

  if (!in_ctx || !out_ctx)
    return 1;

  adjust_ctx(&in_my_dest, ib_port, use_event, gidx, gid, in_ctx);
  adjust_ctx(&out_my_dest, ib_port, use_event, gidx, gid, out_ctx);

  if (rank == START_NODE){
      out_my_dest = get_dest (&out_my_dest, out_rem_dest, servername, port, ib_port, &mtu, sl, gidx, gid, rank, out_ctx, true);  // client = true
    }
  else{
      in_my_dest = get_dest (&in_my_dest, in_rem_dest, servername, port, ib_port, &mtu, sl, gidx, gid, rank, in_ctx, false);    // client = false
    }


  if (rank != START_NODE){
      out_my_dest = get_dest (&out_my_dest, out_rem_dest, servername, port, ib_port, &mtu, sl, gidx, gid, rank, out_ctx, true);  // client = true
    }
  else {
      in_my_dest = get_dest (&in_my_dest, in_rem_dest, servername, port, ib_port, &mtu, sl, gidx, gid, rank, in_ctx, false);    // client = false
    }

  pg_all_reduce(tx_depth, iters, rank, vec_arr, len, tmp_len, in_ctx, out_ctx, op, dt);

//////////////////////////////////////////////////////////////////////////////////

  ibv_free_device_list(dev_list);
  free(in_rem_dest);
  free(out_rem_dest);
  free(vec_arr);
//  pp_close_ctx(in_ctx);
//  pp_close_ctx(out_ctx);
  return 0;
}

#pragma clang diagnostic pop