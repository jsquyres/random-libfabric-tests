#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdarg.h>
#include <assert.h>
#include <sys/epoll.h>

#include <cstdlib>
#include <cinttypes>
#include <map>

#include <mpi.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

using namespace std;

////////////////////////////////////////////////////////////////////////
//
// Global data structures
//

static int comm_rank = -1;
static int comm_size = -1;
static int num_servers = 0;
static uint32_t my_epoll_type = 0;
static const char *id = "UNKNOWN";
static bool hostname_set = false;
static char hostname[4096];


////////////////////////////////////////////////////////////////////////
//
// Logging functions
//

static void logme(const char *msg, ...)
{
    va_list ap;
    char expanded[163860];

     va_start(ap, msg);
     vsnprintf(expanded, sizeof(expanded) - 1, msg, ap);
     va_end(ap);

     if (hostname_set) {
         fprintf(stderr, "%s:%d:MCW %d:%s: %s", hostname, getpid(), comm_rank, id, expanded);
     } else {
         fprintf(stderr, "%s", expanded);
     }
}

#define error(msg) do_error((msg), __LINE__)

static void do_error(const char *msg, int line)
{
     if (hostname_set) {
         fprintf(stderr, "%s:%d:MCW %d:%s:line %d:ERROR: %s",
                 hostname, getpid(), comm_rank, id, line, msg);
     } else {
         fprintf(stderr, "%d:MCW %d:%s:line %d:ERROR: %s",
                 getpid(), comm_rank, id, line, msg);
     }

    MPI_Abort(MPI_COMM_WORLD, 17);
    exit(1);
}

#if 0
// JMS uncomment for debug
static void wait_for_debugger(void)
{
    printf("%s:%s:MCW %d:PID %d: waiting for debugger attach...\n",
           id, hostname, comm_rank, getpid());
    int i = 0;
    while (i == 0) sleep(5);
}
#endif

const char *sprintf_cqe_flags(uint64_t flags)
{
    static char str[8192];

    snprintf(str, sizeof(str), "0x%" PRIx64 ": ", flags);
    if (flags & FI_SEND) strncat(str, "FI_SEND ", sizeof(str));
    if (flags & FI_RECV) strncat(str, "FI_RECV ", sizeof(str));
    if (flags & FI_RMA) strncat(str, "FI_RMA ", sizeof(str));
    if (flags & FI_ATOMIC) strncat(str, "FI_ATOMIC ", sizeof(str));
    if (flags & FI_MSG) strncat(str, "FI_MSG ", sizeof(str));
    if (flags & FI_TAGGED) strncat(str, "FI_TAGGED ", sizeof(str));
    if (flags & FI_READ) strncat(str, "FI_READ ", sizeof(str));
    if (flags & FI_WRITE) strncat(str, "FI_WRITE ", sizeof(str));
    if (flags & FI_REMOTE_READ) strncat(str, "FI_REMOTE_READ ", sizeof(str));
    if (flags & FI_REMOTE_WRITE) strncat(str, "FI_REMOTE_WRITE ", sizeof(str));
    if (flags & FI_REMOTE_CQ_DATA) strncat(str, "FI_REMOTE_CQ_DATA ", sizeof(str));
    if (flags & FI_MULTI_RECV) strncat(str, "FI_MULTI_RECV ", sizeof(str));

    return str;
}

////////////////////////////////////////////////////////////////////////
//
// MPI-related utility functions
//

struct ip_addr_t {
    uint32_t ip_addr;
    uint32_t ip_port_be;
};

// modex = data we exchange (out of band / via MPI) for OFI setup/bootstrapping
struct modex_data_t {
    struct ip_addr_t ip_addr;
};

static modex_data_t *modex_data = NULL;


static void setup_mpi(void)
{
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    int s = sizeof(hostname);
    MPI_Get_processor_name(hostname, &s);
    hostname_set = true;

    if (comm_size < 2) {
        error("Must run at least 2 MPI processes\n");
    }

    // Make client:server ratio be 3:1
    if (comm_size < 4) {
        num_servers = 1;
    } else {
        num_servers = comm_size / 4;
        if (num_servers < 1) {
            error("Must run enough MPI processes to have servers\n");
        }
    }

    // Make things (sorta) repeatable
    srand(comm_rank);
    srand48(comm_rank);
}

static void teardown_mpi(void)
{
    MPI_Finalize();
}

static void modex(struct sockaddr_in &sin)
{
    modex_data_t send_data;
    send_data.ip_addr.ip_addr = sin.sin_addr.s_addr;
    send_data.ip_addr.ip_port_be = sin.sin_port;
    logme("Modex sending: %s:%d\n", inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

    modex_data = new modex_data_t[comm_size];
    assert(modex_data);
    memset(modex_data, 0, sizeof(modex_data_t) * comm_size);

    MPI_Allgather(&send_data, 2, MPI_UINT32_T,
                  modex_data, 2, MPI_UINT32_T, MPI_COMM_WORLD);
}

////////////////////////////////////////////////////////////////////////
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

static device_t fidev;
static const int AV_SIZE = 8192;
static const int CQ_SIZE = 8192;
static int epoll_fd = -1;

static struct fid_mr no_mr = {0};
static int mr_flags = FI_SEND | FI_RECV | FI_READ | FI_WRITE |
    FI_REMOTE_READ | FI_REMOTE_WRITE;

static uint64_t cqe_seq = 0;
static uint64_t msg_seq = 0;

static const size_t DEFAULT_RECEIVE_SIZE = 12000; // JMS semi-arbitrary large-ish number

static const size_t RDMA_SLAB_SIZE = 1024 * 1024 * 12;

// These two functions need to be defined this far down because it
// uses data structures and global variables defined just above.
static void log_inbound_msg(msg_t *msg, const char *label)
{
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = msg->from_ip.ip_addr;
    addr_sin.sin_port = msg->from_ip.ip_port_be;

    logme("Got %s from %s:%d (MCW rank %d), seq=%" PRIu64 "\n",
          label,
          inet_ntoa(addr_sin.sin_addr),
          ntohs(addr_sin.sin_port),
          msg->from_mcw_rank,
          msg->seq);
}

static void log_outbound_msg(int mcw_rank, const char *label)
{
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = modex_data[mcw_rank].ip_addr.ip_addr;
    addr_sin.sin_port = modex_data[mcw_rank].ip_addr.ip_port_be;

    logme("Sending %s to %s:%d (MCW rank %d)\n",
          label,
          inet_ntoa(addr_sin.sin_addr),
          ntohs(addr_sin.sin_port),
          mcw_rank);
}

////////////////////////////////////////////////////////////////////////
//
// Epoll utility functions
//

static void add_epoll_fd(int fd)
{
    // This is a memory leak; I know.  Good enough for a small test.
    struct epoll_event *edt;
    edt = (struct epoll_event*) calloc(1, sizeof(*edt));
    assert(edt != NULL);

    edt->events = EPOLLIN;
    edt->data.u32 = my_epoll_type;
    int ret;
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, edt);
    if (ret < 0) {
        error("epoll_ctl failed");
    }
}

static void del_epoll_fd(int fd)
{
    // This is a memory leak; I know.  Good enough for a small test.
    struct epoll_event *edt;
    edt = (struct epoll_event*) calloc(1, sizeof(edt));
    assert(edt != NULL);

    edt->events = EPOLLIN;
    edt->data.u32 = my_epoll_type;
    int ret;
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, edt);
    if (ret < 0) {
        error("epoll_ctl failed");
    }
}

static void wait_for_epoll(void)
{
    // Now wait for the listen to complete
    int nevents;
#define NEVENTS 32
    struct epoll_event events[NEVENTS];
    int timeout = 5000;

    while (1) {
        //logme("blocking on epoll fd: %d\n", epoll_fd);
        nevents = epoll_wait(epoll_fd, events, NEVENTS, timeout);
        //logme("back from epoll\n");
        if (nevents < 0) {
            if (errno != EINTR) {
                error("epoll wait failed");
            } else {
                continue;
            }
        } else if (nevents > 0) {
            //logme("successfully woke up from epoll! %d events\n", nevents);

            // Epoll context sanity check
            for (int i = 0; i < nevents; ++i) {
                if (events[i].data.u32 != my_epoll_type) {
                    error("unexpected epoll return type");
                }
            }

            // Happiness!
            return;
        } else {
            logme("wokeup from epoll due to timeout\n");
        }
    }
}

////////////////////////////////////////////////////////////////////////
//
// Buffer utility functions
//

static void buffer_pattern_fill(uint8_t *ptr, uint64_t len)
{
    for (uint64_t i = 0; i < len; ++i, ++ptr) {
        *ptr = i % 255;
    }
}

static void buffer_pattern_check(uint8_t *ptr, uint64_t len)
{
    bool printed = false;
    uint64_t count = 0;

    for (uint64_t i = 0; i < len; ++i, ++ptr) {
        if (*ptr != i % 255) {
            if (!printed) {
                logme("Unexpected buffer[%" PRIu64 "]=%" PRIu64 ", expected %" PRIu64 " (buffer address %p)\n",
                      i, *ptr, (i % 255), ptr);
                printed = true;
                ++count;
            }
        }
    }

    if (count > 0) {
        logme("Ran into %" PRIu64 " more errors (out of %" PRIu64 ")\n",
              count - 1, len);
    }
#if 0
    // JMS :-(
    if (count > 0) {
        error("Cannot continue\n");
    }
#endif
}

////////////////////////////////////////////////////////////////////////
//
// OFI utility functions
//

static void setup_ofi_device(void)
{
    memset(&fidev, 0, sizeof(fidev));

    struct fi_fabric_attr fabric_attr;
    memset(&fabric_attr, 0, sizeof(fabric_attr));
    fabric_attr.prov_name = (char*) "sockets";
    //fabric_attr.prov_name = (char*) "usnic";
    struct fi_ep_attr ep_attr;
    memset(&ep_attr, 0, sizeof(ep_attr));
    ep_attr.type = FI_EP_RDM;

    struct fi_info hints;
    memset(&hints, 0, sizeof(hints));
    hints.caps = FI_MSG;
    hints.mode = FI_LOCAL_MR;
    hints.addr_format = FI_SOCKADDR_IN;
    hints.ep_attr = &ep_attr;
    hints.fabric_attr = &fabric_attr;

    /* Get a minimum of libfabric v1.3.0.  There were bugs in prior
       versions; might as well start with the current libfabric
       version. */
    uint32_t libfabric_api;
    libfabric_api = FI_VERSION(1, 3);
    int ret;
    ret = fi_getinfo(libfabric_api, NULL, NULL, 0, &hints, &fidev.info);
    if (0 != ret) {
        error("cannot fi_getinfo");
    }

    int num_devs = 0;
    for (struct fi_info *info = fidev.info;
         NULL != info; info = info->next) {
        ++num_devs;
    }
    if (0 == num_devs) {
        error("no fi devices available");
    }

    //logme("INFO: %s\n", fi_tostr(fidev.info, FI_TYPE_INFO));

    // Be clear that we'll take any port
    struct sockaddr_in *sin;
    sin = (struct sockaddr_in*) fidev.info->src_addr;
    sin->sin_port = 0;

    // Make a fabric from first info returned
    // Ensure we're in "basic" MR mode!
    fidev.info->domain_attr->mr_mode = FI_MR_BASIC;
    ret = fi_fabric(fidev.info->fabric_attr, &fidev.fabric, NULL);
    if (0 != ret) {
        error("fi_fabric failed");
    }

    // Make a domain
    ret = fi_domain(fidev.fabric, fidev.info, &fidev.domain, NULL);
    if (0 != ret) {
        error("fi_domain failed");
    }

    // Make an AV
    struct fi_av_attr av_attr;
    memset(&av_attr, 0, sizeof(av_attr));
    av_attr.type = FI_AV_UNSPEC;
    av_attr.count = AV_SIZE;
    ret = fi_av_open(fidev.domain, &av_attr, &fidev.av, NULL);
    if (0 != ret) {
        error("fi_av_open failed");
    }

    // Make an EQ
    struct fi_eq_attr eq_attr;
    memset(&eq_attr, 0, sizeof(eq_attr));
    eq_attr.wait_obj = FI_WAIT_FD;
    ret = fi_eq_open(fidev.fabric, &eq_attr, &fidev.eq, NULL);
    if (0 != ret) {
        error("fi_eq failed");
    }

    // Get the fd associated with this EQ
    ret = fi_control(&(fidev.eq->fid), FI_GETWAIT, &fidev.eq_fd);
    if (ret < 0) {
        error("fi_control to get eq fq failed");
    }

    // Make an epoll fd to listen on
    epoll_fd = epoll_create(4096);
    if (epoll_fd < 0) {
        error("epoll_create failed");
    }
    logme("made epoll fd: %d\n", epoll_fd);

    add_epoll_fd(fidev.eq_fd);
}

static void setup_ofi_endpoint(endpoint_t &ep)
{
    memset(&ep, 0, sizeof(ep));

    // Make an endpoint
    int ret;
    ret = fi_endpoint(fidev.domain, fidev.info, &ep.ep, NULL);
    if (0 != ret) {
        error("fi_endpoint failed");
    }

    // Bind the EQ to the endpoint
    ret = fi_ep_bind(ep.ep, &fidev.eq->fid, 0);
    if (0 != ret) {
        error("fi_ep_bind(eq) failed");
    }

    // Bind the AV to the endpoint
    ret = fi_ep_bind(ep.ep, &(fidev.av->fid), 0);
    if (0 != ret) {
        error("fi_ep_bind(av) failed");
    }

    // Make a CQ
    struct fi_cq_attr cq_attr;
    memset(&cq_attr, 0, sizeof(cq_attr));
    cq_attr.format = FI_CQ_FORMAT_MSG;
    cq_attr.wait_obj = FI_WAIT_FD;
    cq_attr.size = CQ_SIZE;
    ret = fi_cq_open(fidev.domain, &cq_attr, &ep.cq, NULL);
    if (ret != 0) {
        error("fi_cq_open failed");
    }

    // Bind the CQ TX and RX queues to the EQ
    ret = fi_ep_bind(ep.ep, &ep.cq->fid, FI_TRANSMIT);
    if (0 != ret) {
        error("fi_ep_bind(cq tx) failed");
    }
    ret = fi_ep_bind(ep.ep, &ep.cq->fid, FI_RECV);
    if (0 != ret) {
        error("fi_ep_bind(cq rx) failed");
    }

    // Get the fd associated with this CQ
    ret = fi_control(&(ep.cq->fid), FI_GETWAIT, &ep.cq_fd);
    if (ret != 0) {
        error("fi_control to get cq fq failed");
    }
    add_epoll_fd(ep.cq_fd);

    // Enable the EP!
    ret = fi_enable(ep.ep);
    if (0 != ret) {
        error("fi_enable failed");
    }

    // Get the actual address of my EP
    size_t s = sizeof(ep.my_sin);
    ret = fi_getname(&(ep.ep->fid), &(ep.my_sin), &s);
    if (0 != ret) {
        error("fi_getname failed");
    }

    logme("My endpoint is %s:%d\n",
          inet_ntoa(ep.my_sin.sin_addr), ntohs(ep.my_sin.sin_port));
}

static void setup_ofi_rdma_slab(endpoint_t &ep)
{
    ep.rdma_slab = new uint8_t[RDMA_SLAB_SIZE];
    assert(ep.rdma_slab);
    memset(ep.rdma_slab, 0, RDMA_SLAB_SIZE);

    int ret;
    memset(&(ep.rdma_slab_mr), 0, sizeof(ep.rdma_slab_mr));
    ret = fi_mr_reg(fidev.domain, ep.rdma_slab, RDMA_SLAB_SIZE,
                    mr_flags, 0, (uintptr_t) ep.rdma_slab,
                    0, &(ep.rdma_slab_mr), NULL);
    if (ret != 0) {
        logme("ERROR: fi_mr_reg ret=%d, %s\n", fi_strerror(-ret));
        error("fi_mr_reg(rdma) failed");
    }

    logme("My RDMA slab: %p - %p (len %" PRIu64 ")\n",
          ep.rdma_slab, (ep.rdma_slab + RDMA_SLAB_SIZE),
          RDMA_SLAB_SIZE);
}

static void teardown_ofi_rdma_slab(endpoint_t &ep)
{
    if (ep.rdma_slab) {
        fi_close(&(ep.rdma_slab_mr->fid));
        delete[] ep.rdma_slab;
    }
}

static void teardown_ofi_endpoint(endpoint_t &ep)
{
    fi_close(&(ep.ep->fid));

    if (ep.receive_buffers) {
        delete[] ep.receive_buffers;
    }

    del_epoll_fd(ep.cq_fd);
    fi_close(&(ep.cq->fid));
}

static void teardown_ofi_device(void)
{
    del_epoll_fd(fidev.eq_fd);
    close(epoll_fd);

    fi_close(&(fidev.av->fid));
    fi_close(&(fidev.domain->fid));
    fi_close(&(fidev.eq->fid));
    fi_freeinfo(fidev.info);
}

static void wait_for_cq(struct fid_cq *cq, struct fi_cq_msg_entry &cqe_out)
{
    int ret;
    struct fi_cq_msg_entry cqe;

    while (1) {
        wait_for_epoll();

        ret = fi_cq_read(cq, &cqe, 1);
        if (-FI_EAGAIN == ret) {
            logme("woke up on cq fd, but nothing to read...\n");
            continue;
        } else if (-FI_EAVAIL == ret) {
            struct fi_cq_err_entry cee;
            ret = fi_cq_readerr(cq, &cee, 0);
            if (-FI_EAGAIN == ret) {
                logme("===== there's nothing on the error queue!\n");
                continue;
            } else if (-FI_EAVAIL == ret) {
                logme("===== got EAVAIL from cq_readerr\n");
                continue;
            } else {
                logme("===== got error from cq: %d, %s\n",
                      cee.err, fi_strerror(cee.err));
                cqe_context_t *cqec = (cqe_context_t*) cee.op_context;
                assert(cqec);
                assert(CQEC_WRITE == cqec->type);
                logme("===== error on RDMA WRITE, seq=%" PRIu64 "\n", cqec->seq);
                error("===== cannot continue\n");
                continue;
            }
        } else if (ret != 1) {
            error("===== got wrong number of events from fi_cq_read\n");
        }

#if 0
        // JMS debugging
        logme("got completion: flags %s\n", sprintf_cqe_flags(cqe.flags));
        if ((cqe.flags & FI_SEND) && (cqe.flags & FI_MSG)) {
            logme("completed send\n");
        } else if ((cqe.flags & FI_RECV) && (cqe.flags & FI_MSG)) {
            logme("completed recv\n");
        } else if (cqe.flags & FI_WRITE) {
            logme("completed RDMA write\n");
        } else {
            logme("====== got some unknown completion!\n");
        }
#endif

        cqe_out = cqe;
        return;
    }
}

////////////////////////////////////////////////////////////////////////

static void post_receive(endpoint_t &ep, uint8_t *receive_buffer, void *context)
{
    int ret;
    ret = fi_recv(ep.ep, receive_buffer, ep.receive_buffer_size,
                  fi_mr_desc(&no_mr), 0, context);
    if (ret < 0) {
        logme("fi_recv failed! %d, %s\n", ret, fi_strerror(-ret));
        error("cannot continue");
    }
}

static void post_receives(endpoint_t &ep)
{
    ep.receive_buffer_count = fidev.info->rx_attr->size;
    ep.receive_buffer_size =
        DEFAULT_RECEIVE_SIZE < fidev.info->ep_attr->max_msg_size ?
        DEFAULT_RECEIVE_SIZE : fidev.info->ep_attr->max_msg_size;

    ep.receive_buffers_total_size =
        ep.receive_buffer_count * ep.receive_buffer_size;
    ep.receive_buffers = new uint8_t[ep.receive_buffers_total_size];
    assert(ep.receive_buffers);

    ep.cqe_contexts = new cqe_context_t[ep.receive_buffer_count];
    assert(ep.cqe_contexts);
    memset(ep.cqe_contexts, 0, ep.receive_buffer_count * sizeof(cqe_context_t));

    uint8_t *ptr = (uint8_t*) ep.receive_buffers;
    for (uint32_t i = 0; i < ep.receive_buffer_count; ++i) {
        ep.cqe_contexts[i].type = CQEC_RECV;
        ep.cqe_contexts[i].seq = 0;
        ep.cqe_contexts[i].buffer = ptr;
        ep.cqe_contexts[i].mr = NULL;

        post_receive(ep, ptr, &(ep.cqe_contexts[i]));
        ptr += ep.receive_buffer_size;
    }
    logme("Posted %d receives\n", ep.receive_buffer_count);
}

static void msg_fill_header(endpoint_t &ep, msg_t *msg, msg_type_t type)
{
    msg->type = type;
    msg->seq = msg_seq++;
    msg->from_mcw_rank = comm_rank;
    msg->from_ip.ip_addr = (uint32_t) ep.my_sin.sin_addr.s_addr;
    msg->from_ip.ip_port_be = (uint32_t) ep.my_sin.sin_port;

    logme("Fill header: from mcw=%d, from ip=%s:%d\n",
          msg->from_mcw_rank,
          inet_ntoa(ep.my_sin.sin_addr),
          ntohs(msg->from_ip.ip_port_be));
}

static uint64_t msg_send(endpoint_t &ep, msg_t *msg, fi_addr_t peer_fi)
{
    // Make a CQ context entry
    cqe_context_t *cqec = new cqe_context_t;
    assert(cqec);
    cqec->type = CQEC_SEND;
    uint64_t my_seq;
    my_seq = cqe_seq++;
    cqec->seq = my_seq;
    cqec->buffer = (uint8_t*) msg;

    // Send the message
    logme("Sending to fi_addr 0x%" PRIx64 "\n", peer_fi);
    fi_send(ep.ep, msg, sizeof(*msg), fi_mr_desc(&no_mr), peer_fi, cqec);

    return my_seq;
}

static void msg_verify(endpoint_t &ep, msg_t *msg)
{
    if (msg->to_ip.ip_addr != ep.my_sin.sin_addr.s_addr ||
        msg->to_ip.ip_port_be != ep.my_sin.sin_port) {
        const char *my_ip = strdup(inet_ntoa(ep.my_sin.sin_addr));
        struct sockaddr_in sin;
        sin.sin_family = AF_INET;
        sin.sin_addr.s_addr = msg->from_ip.ip_addr;
        const char *from_ip = strdup(inet_ntoa(sin.sin_addr));
        sin.sin_addr.s_addr = msg->to_ip.ip_addr;
        const char *to_ip = strdup(inet_ntoa(sin.sin_addr));

        logme("ERROR: %s:%d got a message of type %d from %s:%d that was really bound for %s:%d!!\n",
              my_ip, ntohs(ep.my_sin.sin_port),
              msg->type,
              from_ip, ntohs(msg->from_ip.ip_port_be),
              to_ip, ntohs(msg->to_ip.ip_port_be));
        error("*********** TEST FAIL!\n");
    }
}

////////////////////////////////////////////////////////////////////////

// For servers only
struct client_info_t {
    struct sockaddr_in   addr_sin; // just for our reference
    int                  mcw_rank; // just for our reference

    ip_addr_t            addr_ip;  // used to address msg_t's to client
    fi_addr_t            addr_fi;  // used to fi_send/fi_write to client
    uint64_t             rdma_key; // used to fi_write to client
};

typedef map<struct sockaddr_in, client_info_t> client_map_t;

static client_map_t server_map_of_clients;

// Provide operator<() for sockaddr_in so that we can use it as the
// key in an stl::map.
static bool operator<(const sockaddr_in a, const sockaddr_in b)
{
    if (a.sin_family < b.sin_family) {
        return true;
    } else if (a.sin_family > b.sin_family) {
        return false;
    } else if (a.sin_addr.s_addr < b.sin_addr.s_addr) {
        return true;
    } else if (a.sin_addr.s_addr > b.sin_addr.s_addr) {
        return false;
    } else if (a.sin_port > b.sin_port) {
        return true;
    } else {
        return false;
    }
}

static void server_handle_rdma_completion(struct fi_cq_msg_entry &cqe)
{
    // We completed an RDMA write.  Yay!
    logme("Completed an RDMA write\n");

    // Get the CQEC
    assert(cqe.op_context);
    cqe_context_t *cqec;
    cqec = (cqe_context_t*) cqe.op_context;

    // Verify that this cqec is valid
    assert(cqec);
    assert(CQEC_WRITE == cqec->type);
    assert(cqec->buffer);
    assert(cqec->mr);

    // De-register the RDMA memory
    fi_close(&(cqec->mr->fid));

    // Delete the RDMA buffer and the CQEC
    delete[] cqec->buffer;
    delete cqec;
}

static void server_handle_send_completion(struct fi_cq_msg_entry &cqe)
{
    // We completed a send.  Yay!
    logme("Completed a send\n");

    // Get the CQEC
    assert(cqe.op_context);
    cqe_context_t *cqec;
    cqec = (cqe_context_t*) cqe.op_context;

    // Verify that this cqec is valid
    assert(cqec);
    assert(CQEC_SEND == cqec->type);
    assert(cqec->buffer);

    // Delete the send buffer and the CQEC
    delete cqec->buffer;
    delete cqec;
}

static void server_handle_connect(struct endpoint_t &ep, msg_t *msg)
{
    log_inbound_msg(msg, "MSG_CONNECT");

    // Make sure we don't already know this client
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = msg->from_ip.ip_addr;
    addr_sin.sin_port = msg->from_ip.ip_port_be;

    client_map_t::iterator it;
    it = server_map_of_clients.find(addr_sin);
    if (server_map_of_clients.end() != it) {
        error("...but we're already connected\n");
    }

    // Resolve the client address (so we can send to it)
    int ret;
    fi_addr_t addr_fi;
    ret = fi_av_insert(fidev.av, &addr_sin, 1, &addr_fi, 0, NULL);
    assert(1 == ret);
    logme("fi_av_insert of %s:%d --> fi_addr_t 0x%" PRIx64 "\n",
          inet_ntoa(addr_sin.sin_addr), htons(addr_sin.sin_port), addr_fi);

    // Save the client info in the client map
    struct client_info_t cinfo;
    cinfo.addr_sin = addr_sin;
    cinfo.addr_fi = addr_fi;
    cinfo.addr_ip = msg->from_ip;
    cinfo.rdma_key = msg->u.connect.client_rdma_key;
    cinfo.mcw_rank = msg->from_mcw_rank;
    server_map_of_clients[addr_sin] = cinfo;

    // Send back a CONNECT_ACK
    // Fill header of message
    msg_t *to_client = new msg_t;
    assert(to_client);
    msg_fill_header(ep, to_client, MSG_CONNECT_ACK);
    to_client->to_ip = cinfo.addr_ip;

    // Fill payload of message
    // JMS For the moment, not initiating RDMA client --> server.
    // Just send back a bogus key.
    to_client->u.connect_ack.server_rdma_key = 0;

    // Send the CONNECT_ACK (no need to wait for it here)
    logme("Sending CONNECT_ACK back to %s:%d, fi_addr 0x%" PRIx64 "\n",
          inet_ntoa(cinfo.addr_sin.sin_addr),
          ntohs(cinfo.addr_sin.sin_port),
          cinfo.addr_fi);
    msg_send(ep, to_client, addr_fi);
    logme("Sent CONNECT_ACK back to %s:%d, fi_addr 0x%" PRIx64 "\n",
          inet_ntoa(cinfo.addr_sin.sin_addr),
          ntohs(cinfo.addr_sin.sin_port),
          cinfo.addr_fi);
}

static void server_handle_disconnect(struct endpoint_t &ep, msg_t *msg)
{
    log_inbound_msg(msg, "MSG_DISCONNECT");

    // Make sure we already know this client
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = msg->from_ip.ip_addr;
    addr_sin.sin_port = msg->from_ip.ip_port_be;

    client_map_t::iterator it;
    it = server_map_of_clients.find(addr_sin);
    if (server_map_of_clients.end() == it) {
        error("...but we don't know who this client is!\n");
    }

    // Remove this client from the fi_av
    int ret;
    ret = fi_av_remove(fidev.av, &(it->second.addr_fi), 1, 0);
    assert(0 == ret);
    logme("fi_av_remove of %s:%d --> fi_addr_t 0x%" PRIx64 "\n",
          inet_ntoa(it->second.addr_sin.sin_addr),
          htons(it->second.addr_sin.sin_port),
          it->second.addr_fi);

    // Remove this client from the server_map_of_clients
    server_map_of_clients.erase(it);
}

static void server_handle_solicit_rdma(struct endpoint_t &ep, msg_t *msg)
{
    log_inbound_msg(msg, "SOLICIT_RDMA");

    // Make sure we already know this client
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = msg->from_ip.ip_addr;
    addr_sin.sin_port = msg->from_ip.ip_port_be;

    client_map_t::iterator it;
    it = server_map_of_clients.find(addr_sin);
    if (server_map_of_clients.end() == it) {
        error("...but we don't know who this client is!\n");
    }

    // Randomize the amount of data we're going to RDMA back.  Use the
    // client-requested size as the upper bound.
    size_t len;
    len = msg->u.solicit_rdma.client_rdma_target_len;
    len *= drand48();
    if (0 == len) {
        len = msg->u.solicit_rdma.client_rdma_target_len;
    }

    // Make a buffer to RDMA write, and fill it with a predictable
    // pattern
    uint8_t *rdma_buffer = new uint8_t[len];
    assert(rdma_buffer);
    buffer_pattern_fill(rdma_buffer, len);

    // Make a context to store info about the RDMA buffer (so we can
    // free it when the RDMA write is complete)
    cqe_context_t *cqec = new cqe_context_t;
    assert(cqec);
    memset(cqec, 0, sizeof(*cqec));
    cqec->type = CQEC_WRITE;
    cqec->seq = cqe_seq++;
    cqec->buffer = rdma_buffer;

    int ret;
    ret = fi_mr_reg(fidev.domain, rdma_buffer, len,
                    mr_flags, 0, (uintptr_t) rdma_buffer,
                    0, &(cqec->mr), NULL);
    if (ret != 0) {
        logme("ERROR: fi_mr_reg ret=%d, %s\n", fi_strerror(-ret));
        error("fi_mr_reg(rdma) failed");
    }

    // Do the RDMA write
    logme("RDMA writing to %s:%d (fi_addr 0x%" PRIx64 ", MCW rank %d), dest buffer=%p - %p (len=%" PRIu64 "), seq=%" PRIu64 ", dest key=%" PRIx64 "\n",
          inet_ntoa(it->second.addr_sin.sin_addr),
          ntohs(it->second.addr_sin.sin_port),
          it->second.addr_fi,
          it->second.mcw_rank,
          msg->u.solicit_rdma.client_rdma_target_addr,
          msg->u.solicit_rdma.client_rdma_target_addr + len,
          len,
          cqec->seq,
          it->second.rdma_key);
    ssize_t fi_write(struct fid_ep *ep, const void *buf, size_t len,
                     void *desc, fi_addr_t dest_addr, uint64_t addr, uint64_t key,
                     void *context);

    ret = fi_write(ep.ep, rdma_buffer, len, fi_mr_desc(cqec->mr),
                   it->second.addr_fi,
                   msg->u.solicit_rdma.client_rdma_target_addr,
                   it->second.rdma_key,
                   cqec);
    assert(0 == ret);

    // Send back a RDMA_SENT
    // Fill header of message
    msg_t *to_client = new msg_t;
    assert(to_client);
    msg_fill_header(ep, to_client, MSG_RDMA_SENT);
    to_client->to_ip = it->second.addr_ip;

    // Fill payload of message
    to_client->u.rdma_sent.client_rdma_target_addr =
        msg->u.solicit_rdma.client_rdma_target_addr;
    to_client->u.rdma_sent.client_rdma_actual_len = len;

    // Send it (no need to wait for it here)
    msg_send(ep, to_client, it->second.addr_fi);
    logme("Sent RDMA_SENT back to %s:%d, fi_addr 0x%" PRIx64 "\n",
          inet_ntoa(it->second.addr_sin.sin_addr),
          ntohs(it->second.addr_sin.sin_port),
          it->second.addr_fi);
}

static void server_handle_receive(struct endpoint_t &ep, struct fi_cq_msg_entry &cqe)
{
    cqe_context_t *cqec;
    cqec = (cqe_context_t*) cqe.op_context;
    assert(cqec->type == CQEC_RECV);

    msg_t *msg = (msg_t*) cqec->buffer;
    msg_verify(ep, msg);

    switch(msg->type) {
    case MSG_CONNECT:
        server_handle_connect(ep, msg);
        break;

    case MSG_DISCONNECT:
        server_handle_disconnect(ep, msg);
        break;

    case MSG_SOLICIT_RDMA:
        server_handle_solicit_rdma(ep, msg);
        break;

    default:
        logme("Got unexpected message type: %d\n", msg->type);
        error("cannot continue");
    }

    // Repost the receive buffer
    post_receive(ep, cqec->buffer, (void*) cqec);
}

//
// Main Server function
//
static void server_main()
{
    setup_ofi_device();
    endpoint_t ep;
    setup_ofi_endpoint(ep);

    // Post receives
    post_receives(ep);

    // Broadcast my OFI endpoint IP address+port to all clients
    modex(ep.my_sin);

    uint64_t rma_completion = FI_RMA | FI_MSG | FI_SEND;
    uint64_t send_completion = FI_SEND | FI_MSG;
    uint64_t recv_completion = FI_RECV | FI_MSG;

    // Main server loop
    struct fi_cq_msg_entry cqe;
    while (1) {
        wait_for_cq(ep.cq, cqe);
        if (rma_completion == (cqe.flags & rma_completion)) {
            server_handle_rdma_completion(cqe);
        } else if (send_completion == (cqe.flags & send_completion)) {
            server_handle_send_completion(cqe);
        } else if (recv_completion == (cqe.flags & recv_completion)) {
            // We completed a recive.  Yay!
            server_handle_receive(ep, cqe);
        }
    }

    teardown_ofi_endpoint(ep);
    teardown_ofi_device();
}

////////////////////////////////////////////////////////////////////////

//
// If we have an entry in the fi_addr_t cache for a given MCW rank,
// then we're "connected" (meaning: we have some state about this
// peer).
//
static bool client_rank_connected(int rank)
{
    rank_to_fi_addr_map_t::iterator it;
    it = client_rank_to_fi_addr_map.find(rank);
    return (client_rank_to_fi_addr_map.end() == it) ? false : true;
}

static void client_rank_disconnect(int rank)
{
    rank_to_fi_addr_map_t::iterator it;
    it = client_rank_to_fi_addr_map.find(rank);
    if (client_rank_to_fi_addr_map.end() != it) {
        client_rank_to_fi_addr_map.erase(it);
    }
}

//
// Look up a cached fi_addr_t based on an MPI_COMM_WORLD rank.  If we
// don't have it, fi_av_insert() it, and then save the result in the
// cache map for next time.
//
static fi_addr_t client_rank_to_fi_addr(int rank)
{
    if (client_rank_connected(rank)) {
        return client_rank_to_fi_addr_map[rank];
    }

    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = modex_data[rank].ip_addr.ip_addr;
    addr_sin.sin_port = modex_data[rank].ip_addr.ip_port_be;

    int ret;
    fi_addr_t addr_fi;
    ret = fi_av_insert(fidev.av, &addr_sin, 1, &addr_fi, 0, NULL);
    assert(1 == ret);
    logme("fi_av_insert of %s:%d --> fi_addr_t 0x%" PRIx64 "\n",
          inet_ntoa(addr_sin.sin_addr), htons(addr_sin.sin_port), addr_fi);

    client_rank_to_fi_addr_map[rank] = addr_fi;

    return addr_fi;
}

static void client_wait_for_recv(endpoint_t &ep, msg_type_t type,
                                 cqe_context_t *&cqec)
{
    logme("Waiting to receive message of type %d\n", type);

    // Wait for something on the CQ
    struct fi_cq_msg_entry cqe;
    while (1) {
        wait_for_cq(ep.cq, cqe);

        assert(cqe.op_context);
        cqec = (cqe_context_t*) cqe.op_context;

        // If we completed a send, free the buffer and cqec
        if (cqe.flags & FI_SEND) {
            logme("CQ: Completed a send\n");
            delete cqec->buffer;
            delete cqec;
            continue;
        }

        // If we completed an RDMA (should never happen here in the client)
        if ((cqe.flags & FI_RECV) && (cqe.flags & FI_RMA)) {
            logme("CQ: Completed an RDMA\n");
            continue;
        }

        // Make sure it was a receive
        assert(cqe.flags & FI_RECV);
        assert(cqe.flags & FI_MSG);
        logme("CQ: Completed a receive\n");

        assert(cqec->type == CQEC_RECV);
        assert(cqec->seq == 0);

        // Make sure the received message was the right type
        msg_t *msg;
        msg = (msg_t*) cqec->buffer;
        msg_verify(ep, msg);
        assert(msg->type == type);

        // All good!
        return;
    }
}

static void client_hulk_smash(endpoint_t &ep)
{
    // JMS This seems to be problematic, but we're also running into
    // other problems.  So temporarily disable the
    // tear-everything-down-and-rebuild-from-scratch stuff.
    return;



















    logme("Hulk smash!\n");

    // Tear it all down
    teardown_ofi_rdma_slab(ep);
    teardown_ofi_endpoint(ep);
    teardown_ofi_device();

    // Recreate
    setup_ofi_device();
    setup_ofi_endpoint(ep);
    setup_ofi_rdma_slab(ep);
}

static void client_connect(endpoint_t &ep, int server_mcw_rank)
{
    log_outbound_msg(server_mcw_rank, "MSG_CONNECT");

    // Fill header of message
    msg_t *to_server = new msg_t;
    assert(to_server);
    msg_fill_header(ep, to_server, MSG_CONNECT);
    to_server->to_ip = modex_data[server_mcw_rank].ip_addr;

    // Fill payload of message
    to_server->u.connect.client_rdma_key = fi_mr_key(ep.rdma_slab_mr);
    logme("MSG_CONNECT sending my slab RDMA key: %" PRIx64 "\n",
          fi_mr_key(ep.rdma_slab_mr));

    // Send
    fi_addr_t peer_fi;
    peer_fi = client_rank_to_fi_addr(server_mcw_rank);
    msg_send(ep, to_server, peer_fi);

    // Wait for CONNECT_ACK message and completion of the send
    cqe_context_t *cqec;
    logme("Waiting to receive CONNECT_ACK...\n");
    client_wait_for_recv(ep, MSG_CONNECT_ACK, cqec);
    logme("Received CONNECT_ACK -- yay!\n");

    // We don't need any information from the CONNECT_ACK -- just
    // getting it is good enough.

    // Repost the receive
    post_receive(ep, cqec->buffer, cqec);
}

static void client_solicit_rdma(endpoint_t &ep, int server_mcw_rank)
{
    // If we're not already connected, connect
    if (!client_rank_connected(server_mcw_rank)) {
        client_connect(ep, server_mcw_rank);
    }

    log_outbound_msg(server_mcw_rank, "MSG_SOLICIT_RDMA");

    // Reset the RDMA slab to known values
    memset(ep.rdma_slab, 17, RDMA_SLAB_SIZE);

    // Ask for an RDMA to a random chunk in the middle of the slab
    size_t offset = (size_t) ((RDMA_SLAB_SIZE / 2) * drand48());
    // Make sure it is aligned
    offset -= (offset % 8);
    size_t len = (size_t) ((RDMA_SLAB_SIZE / 2) * drand48());
    uint8_t *ptr = ep.rdma_slab + offset;
    assert(ptr < (ep.rdma_slab + RDMA_SLAB_SIZE));
    assert(ptr + len < (ep.rdma_slab + RDMA_SLAB_SIZE));

    // Request some RDMA from the server
    msg_t *to_server = new msg_t;
    assert(to_server);
    msg_fill_header(ep, to_server, MSG_SOLICIT_RDMA);
    to_server->to_ip = modex_data[server_mcw_rank].ip_addr;
    logme("SOLICIT_RDMA buffer: %p - %p, len: %" PRIu64 ", key: 0x%" PRIx64 "\n",
          ptr, (ptr + len), len, fi_mr_key(ep.rdma_slab_mr));

    // Fill payload of message
    to_server->u.solicit_rdma.client_rdma_target_addr = (uint64_t)(uintptr_t) ptr;
    to_server->u.solicit_rdma.client_rdma_target_len = (uint64_t) len;

    // Send it
    fi_addr_t peer_fi;
    peer_fi = client_rank_to_fi_addr(server_mcw_rank);
    msg_send(ep, to_server, peer_fi);

    // Wait for a message back from the server saying that the RDMA to
    // my slab is done
    cqe_context_t *cqec;
    client_wait_for_recv(ep, MSG_RDMA_SENT, cqec);

    // Make sure we got a valid reply from the server
    msg_t *from_server;
    from_server = (msg_t*) cqec->buffer;
    assert(from_server->u.rdma_sent.client_rdma_target_addr ==
           (uint64_t)(uintptr_t) ptr);
    assert(from_server->u.rdma_sent.client_rdma_actual_len <= len);

    logme("Got MSG_RDMA_SENT: server wrote %" PRIu64 " bytes to %p\n",
          from_server->u.rdma_sent.client_rdma_actual_len,
          from_server->u.rdma_sent.client_rdma_target_addr);

    // Check that we got what we think we should have gotten
    buffer_pattern_check(ptr, from_server->u.rdma_sent.client_rdma_actual_len);

    // Repost the receive
    post_receive(ep, cqec->buffer, cqec);
}

static void client_disconnect(endpoint_t &ep, int server_mcw_rank)
{
    // If we're not connected, just return
    if (!client_rank_connected(server_mcw_rank)) {
        return;
    }

    log_outbound_msg(server_mcw_rank, "MSG_DISCONNECT");

    // Fill header of message
    msg_t *to_server = new msg_t;
    assert(to_server);
    msg_fill_header(ep, to_server, MSG_DISCONNECT);
    to_server->to_ip = modex_data[server_mcw_rank].ip_addr;

    // There is no payload for the DISCONNECT message

    // Send and wait for the message
    fi_addr_t peer_fi;
    peer_fi = client_rank_to_fi_addr(server_mcw_rank);
    msg_send(ep, to_server, peer_fi);

    // Actually disconnect us
    client_rank_disconnect(server_mcw_rank);
}

//
// Main Client routine
//
static void client_main()
{
    setup_ofi_device();

    // Get all the servers' OFI endpoints IP addresses+ports.
    // Additionally: in the real application, all server IPs are
    // distributed OOB ahead of time, but client IPs are discovered by
    // servers when the clients send to them.
    struct sockaddr_in sin = {0};
    modex(sin);

    // Create my endpoint and RDMA slab
    endpoint_t ep;
    setup_ofi_endpoint(ep);
    setup_ofi_rdma_slab(ep);

    // Post receives
    post_receives(ep);

    // Main client loop
    double chaos;
    int server_mcw_rank;
    while (1) {
        // Randomly pick a server (all the servers are [0, num_servers) )
        server_mcw_rank = rand() % num_servers;
        assert(server_mcw_rank >= 0 && server_mcw_rank < num_servers);

        // Randomly pick an action
        chaos = drand48();
        if (chaos < 0.05) {
            client_hulk_smash(ep);
        } else if (chaos < 0.25) {
            client_disconnect(ep, server_mcw_rank);
        } else {
            client_solicit_rdma(ep, server_mcw_rank);
        }

        // Wait for a (short) random amount of time to simulate
        // computation
        logme("random usleep\n");
        usleep(drand48() * 100);
    }

    teardown_ofi_endpoint(ep);
    teardown_ofi_device();
}

////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    setup_mpi();
    if (comm_rank < num_servers) {
        id = "SERVER";
        my_epoll_type = 3333;
        server_main();
    } else {
        id = "CLIENT";
        my_epoll_type = 4444;
        client_main();
    }
    teardown_mpi();

    return 0;
}
