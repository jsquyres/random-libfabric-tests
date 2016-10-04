#ifndef OFI_RDM_TEST_ABUSE_H
#define OFI_RDM_TEST_ABUSE_H

#include <map>

using namespace std;


//
// Client and server stuff
//
void server_main();
void client_main();

//
// Global stuff
//
extern int comm_rank;
extern int comm_size;
extern int num_servers;
extern uint32_t my_epoll_type;
extern const char *id;
extern bool hostname_set;
extern char *hostname;
extern int num_interactions;


//
// Logging stuff
//
void logme(const char *msg, ...);
#define error(msg) do_error((msg), __LINE__);
void do_error(const char *msg, int line);


//
// Debugging stuff
//
void wait_for_debugger(void);


//
// MPI stuff
//
struct ip_addr_t {
    uint32_t ip_addr;
    uint32_t ip_port_be;
};

// modex = data we exchange (out of band / via MPI) for OFI setup/bootstrapping
struct modex_data_t {
    struct ip_addr_t ip_addr;
    uint32_t is_server;
};

extern bool i_am_server;
extern bool modex_is_done;
extern modex_data_t *modex_data;

void modex(struct sockaddr_in &sin);

//
// OFI-related data structures
//

// Cached mapping of MPI_COMM_WORLD rank to fi_addr_t (for clients)
typedef map<int, fi_addr_t> rank_to_fi_addr_map_t;

// Type of context on a CQ entry
enum cqe_context_type_t {
    CQEC_SEND,
    CQEC_RECV,
    CQEC_WRITE,

    CQEC_MAX
};

// Context for CQ entries
struct cqe_context_t {
    cqe_context_type_t type;

    int                peer_mcw_rank;
    uint64_t           seq;
    uint8_t           *buffer;
    struct fid_mr     *mr;
};

// "Device"-level OFI attributes
struct device_t {
    struct fi_info           *info;
    struct fid_fabric        *fabric;
    struct fid_domain        *domain;
    struct fid_av            *av;
    struct fid_eq            *eq;
    int                       eq_fd;
};

// JMS: I don't know why, but this causes a segv if
// client_rank_to_fi_addr_map is inside struct device_t. :-(
static rank_to_fi_addr_map_t client_rank_to_fi_addr_map;

// OFI attributes for a specific RDM endpoint
struct endpoint_t {
    struct fid_ep            *ep;
    struct sockaddr_in        my_sin;

    struct fid_cq            *cq;
    int                       cq_fd;

    uint8_t                  *rdma_slab;
    struct fid_mr            *rdma_slab_mr;

    uint8_t                  *receive_buffers;
    uint32_t                  receive_buffer_count;
    size_t                    receive_buffer_size;
    size_t                    receive_buffers_total_size;

    cqe_context_t            *cqe_contexts;
};

// Message types (that OFI clients and servers exchange)
enum msg_type_t {
    MSG_CONNECT,
    MSG_CONNECT_ACK,
    MSG_DISCONNECT,

    MSG_SOLICIT_RDMA,
    MSG_RDMA_SENT,

    MSG_MAX
};

// Messages that OFI clients and servers exchange
struct msg_t {
    enum msg_type_t type;
    uint64_t seq;

    // Sanity: know who we're sending from and to
    int from_mcw_rank;
    ip_addr_t from_ip;
    ip_addr_t to_ip;

    // Message payloads
    union {
        struct {
            uint64_t client_rdma_key;
        } connect;

        struct {
            uint64_t server_rdma_key;
        } connect_ack;

        struct {
            uint64_t client_rdma_target_addr;
            uint64_t client_rdma_target_len;
        } solicit_rdma;

        struct {
            uint64_t client_rdma_target_addr;
            uint64_t client_rdma_actual_len;
        } rdma_sent;
    } u;
};

extern device_t fidev;

extern struct fid_mr no_mr;
extern int mr_flags;

extern uint64_t cqe_seq;
extern uint64_t msg_seq;

extern const size_t DEFAULT_RECEIVE_SIZE;
extern const size_t RDMA_SLAB_SIZE;

void log_inbound_msg(msg_t *msg, const char *label);
void log_outbound_msg(endpoint_t &ep, int mcw_rank, const char *label);


//
// Buffer utility functions
//
void buffer_pattern_fill(uint8_t *ptr, uint64_t len);
void buffer_pattern_check(uint8_t *ptr, uint64_t len);


//
// OFI utility functions
//
void setup_ofi_device(void);
void setup_ofi_endpoint(endpoint_t &ep);
void setup_ofi_rdma_slab(endpoint_t &ep);
void teardown_ofi(endpoint_t &ep);
void teardown_ofi_rdma_slab(endpoint_t &ep);
void teardown_ofi_endpoint(endpoint_t &ep);
void teardown_ofi_device(void);

void wait_for_cq(struct fid_cq *cq, struct fi_cq_msg_entry &cqe_out);

void post_receive(endpoint_t &ep, uint8_t *receive_buffer, void *context);
void post_receives(endpoint_t &ep);

void msg_fill_header(endpoint_t &ep, msg_t *msg, msg_type_t type);
cqe_context_t *msg_send(endpoint_t &ep, msg_t *msg, fi_addr_t peer_fi);
void msg_verify(endpoint_t &ep, msg_t *msg);

#endif // OFI_RDM_TEST_ABUSE_H
