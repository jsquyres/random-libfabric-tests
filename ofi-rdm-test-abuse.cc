#include <stdio.h>
#include <stdlib.h>
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

static int comm_rank = 0;
static int comm_size = 0;
static int num_servers = 0;
static int my_epoll_type = 0;
static const char *id = "UNKNOWN";
static bool hostname_set = false;
static char hostname[4096];


////////////////////////////////////////////////////////////////////////
//
// Logging functions
//

#define error(msg) do_error((msg), __LINE__)

static void logme(const char *msg, ...)
{
    va_list ap;
    char expanded[163860];

     va_start(ap, msg);
     vsnprintf(expanded, sizeof(expanded) - 1, msg, ap);
     va_end(ap);

     if (hostname_set) {
         fprintf(stderr, "%s:MCW %d: %s", hostname, comm_rank, expanded);
     } else {
         fprintf(stderr, "%s", expanded);
     }
}

static void do_error(const char *msg, int line)
{
    fprintf(stderr, "ERROR: %s:MCW %d:line %d: %s\n", id, comm_rank, line, msg);
    MPI_Abort(MPI_COMM_WORLD, 17);
    exit(1);
}

static void wait_for_debugger(void)
{
    printf("%s:%s:MCW %d:PID %d: waiting for debugger attach...\n",
           id, hostname, comm_rank, getpid());
    int i = 0;
    while (i == 0) sleep(5);
}

const char *sprintf_cqe_flags(uint64_t flags)
{
    static char str[8192];

    snprintf(str, sizeof(str), "0x%x: ", flags);
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
    uint32_t ip_port;
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

    // Make client:server ratio be 3:1
    num_servers = comm_size / 4;
    if (num_servers < 1) {
        error("Must run enough MPI processes to have servers");
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
    send_data.ip_addr.ip_port = sin.sin_port;

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

// Cached mapping of MPI_COMM_WORLD rank to fi_addr_t
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

    uint64_t seq;
    uint8_t *buffer;
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
// rank_to_fi_addr_map is inside struct device_t. :-(
rank_to_fi_addr_map_t     rank_to_fi_addr_map;

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

    // Sanity: know who we're sending from and to
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

static const size_t DEFAULT_RECEIVE_SIZE = 12000; // JMS semi-arbitrary large-ish number

static const size_t RDMA_SLAB_SIZE = 1024 * 1024 * 12;

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
        logme("%s blocking on epoll fd: %d\n", id, epoll_fd);
        nevents = epoll_wait(epoll_fd, events, NEVENTS, timeout);
        logme("%s back from epoll\n", id);
        if (nevents < 0) {
            if (errno != EINTR) {
                error("epoll wait failed");
            } else {
                continue;
            }
        } else if (nevents > 0) {
            logme("%s successfully woke up from epoll! %d events\n",
                  id, nevents);

            // Epoll context sanity check
            for (int i = 0; i < nevents; ++i) {
                if (events[i].data.u32 != my_epoll_type) {
                    error("unexpected epoll return type");
                }
            }

            // Happiness!
            return;
        } else {
            logme("%s wokeup from epoll due to timeout\n", id);
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
                logme("Unexpected buffer[%" PRIu64 "]=%" PRIu64 ", expected %" PRIu64 "\n",
                      i, *ptr, (i % 255));
                printed = true;
                ++count;
            }
        }
    }

    if (count > 1) {
        logme("Ran into %" PRIu64 " more errors (out of %" PRIu64 "\n");
    }
    if (count > 0) {
        error("Cannot continue\n");
    }
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
    logme("%s made epoll fd: %d\n", id, epoll_fd);

    add_epoll_fd(fidev.eq_fd);
}

static void setup_ofi_endpoint(endpoint_t &ep, bool setup_rdma_slab)
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

    // Setup RDMA (if requested)
    if (setup_rdma_slab) {
        ep.rdma_slab = new uint8_t[RDMA_SLAB_SIZE];
        assert(ep.rdma_slab);
        memset(ep.rdma_slab, 0, RDMA_SLAB_SIZE);
        ret = fi_mr_reg(fidev.domain, ep.rdma_slab, RDMA_SLAB_SIZE,
                        mr_flags, 0, (uintptr_t) ep.rdma_slab,
                        0, &(ep.rdma_slab_mr), NULL);
        if (ret != 0) {
            logme("ERROR: fi_mr_reg ret=%d, %s\n", fi_strerror(-ret));
            error("fi_mr_reg(rdma) failed");
        }
    }
}

static void teardown_ofi_endpoint(endpoint_t &ep)
{
    if (ep.rdma_slab) {
        fi_close(&(ep.rdma_slab_mr->fid));
        delete[] ep.rdma_slab;
    }
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

static void wait_for_cq(const char *id, struct fid_cq *cq,
                        struct fi_cq_msg_entry cqe_out)
{
    int ret;
    struct fi_cq_msg_entry cqe;

    while (1) {
        wait_for_epoll();

        ret = fi_cq_read(cq, &cqe, 1);
        if (-FI_EAGAIN == ret) {
            logme("%s woke up on cq fd, but nothing to read...\n", id);
            continue;
        } else if (-FI_EAVAIL == ret) {
            logme("%s ===== woke up on cq fd, but there's something on the error queue!\n", id);
            struct fi_cq_err_entry cee;
            ret = fi_cq_readerr(cq, &cee, 0);
            if (-FI_EAGAIN == ret) {
                logme("%s ===== there's nothing on the error queue!\n", id);
                continue;
            } else if (-FI_EAVAIL == ret) {
                logme("%s ===== got EAVAIL from cq_readerr\n", id);
                continue;
            } else {
                logme("%s ===== got error from cq: %d, %s\n",
                      cee.err, fi_strerror(cee.err), id);
                continue;
            }
        } else if (ret != 1) {
            error("===== got wrong number of events from fi_cq_read\n");
        }

        logme("%s got completion: flags %s\n",
              sprintf_cqe_flags(cqe.flags), id);
        if ((cqe.flags & FI_SEND) && (cqe.flags & FI_MSG)) {
            logme("%s completed send\n", id);
        } else if ((cqe.flags & FI_RECV) && (cqe.flags & FI_MSG)) {
            logme("%s completed recv\n", id);
        } else if (cqe.flags & FI_WRITE) {
            logme("%s completed RDMA write\n", id);
        } else {
            logme("%s ====== got some unknown completion!\n", id);
        }

        cqe_out = cqe;
        return;
    }
}

//
// If we have an entry in the fi_addr_t cache for a given MCW rank,
// then we're "connected" (meaning: we have some state about this
// peer).
//
static bool rank_connected(int rank)
{
    rank_to_fi_addr_map_t::iterator it;
    it = rank_to_fi_addr_map.find(rank);
    return (rank_to_fi_addr_map.end() == it) ? false : true;
}

//
// Look up a cached fi_addr_t based on an MPI_COMM_WORLD rank.  If we
// don't have it, fi_av_insert() it, and then save the result in the
// cache map for next time.
//
static fi_addr_t rank_to_fi_addr(int rank)
{
    if (rank_connected(rank)) {
        return rank_to_fi_addr_map[rank];
    }

    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = modex_data[comm_rank].ip_addr.ip_addr;
    addr_sin.sin_port = modex_data[comm_rank].ip_addr.ip_port;

    int ret;
    fi_addr_t addr_fi;
    ret = fi_av_insert(fidev.av, &addr_sin, 1, &addr_fi, 0, NULL);
    assert(ret == 1);

    rank_to_fi_addr_map[rank] = addr_fi;

    return addr_fi;
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
    for (int i = 1; i < ep.receive_buffer_count; ++i) {
        ep.cqe_contexts[i].type = CQEC_RECV;
        ep.cqe_contexts[i].seq = 0;
        ep.cqe_contexts[i].buffer = ptr;

        post_receive(ep, ptr, &(ep.cqe_contexts[i]));
        ptr += ep.receive_buffer_size;
    }
}

static void msg_fill_header(msg_t &msg, msg_type_t type, int target_mcw_rank)
{
    msg.type = type;
    msg.from_ip = modex_data[comm_rank].ip_addr;
    msg.to_ip = modex_data[target_mcw_rank].ip_addr;
}

static void msg_send_and_wait(endpoint_t &ep, msg_t &msg,
                              int target_mcw_rank)
{
    // Make a CQ context entry
    cqe_context_t cqec;
    cqec.type = CQEC_SEND;
    uint64_t my_seq;
    my_seq = cqe_seq++;
    cqec.seq = my_seq;

    // Send the message
    fi_addr_t server_fi;
    server_fi = rank_to_fi_addr(target_mcw_rank);
    fi_send(ep.ep, &msg, sizeof(msg),
            fi_mr_desc(&no_mr), server_fi, &cqec);

    // Wait for the send completion
    struct fi_cq_msg_entry cqe;
    wait_for_cq(id, ep.cq, cqe);
    assert(cqe.flags & FI_SEND);
    assert(cqe.flags & FI_MSG);
    assert(cqe.op_context == &cqec);
    assert(cqec.type == CQEC_SEND);
    assert(cqec.seq == my_seq);
}

static void msg_wait_for_recv(endpoint_t &ep, msg_type_t type,
                              cqe_context_t *&cqec)
{
    // Wait for something on the CQ
    struct fi_cq_msg_entry cqe;
    wait_for_cq(id, ep.cq, cqe);

    // Make sure it was a receive
    assert(cqe.flags & FI_RECV);
    assert(cqe.flags & FI_MSG);

    assert(cqe.op_context);
    cqec = (cqe_context_t*) cqe.op_context;
    assert(cqec->type == CQEC_RECV);
    assert(cqec->seq == 0);

    // Make sure the message was actually for me
    // (**** THIS IS THE MAIN PART OF THE TEST ***)
    msg_t *msg;
    msg = (msg_t*) cqec->buffer;
    if (msg->to_ip.ip_addr != ep.my_sin.sin_addr.s_addr ||
        msg->to_ip.ip_port != ep.my_sin.sin_port) {
        const char *my_ip = strdup(inet_ntoa(ep.my_sin.sin_addr));
        struct sockaddr_in sin;
        sin.sin_family = AF_INET;
        sin.sin_addr.s_addr = msg->from_ip.ip_addr;
        const char *from_ip = strdup(inet_ntoa(sin.sin_addr));
        sin.sin_addr.s_addr = msg->to_ip.ip_addr;
        const char *to_ip = strdup(inet_ntoa(sin.sin_addr));
        logme("ERROR!  %s:%d got a message from %s:%d that was really bound for %s:%d!!",
              my_ip, ntohs(ep.my_sin.sin_port),
              from_ip, ntohs(msg->from_ip.ip_port),
              to_ip, ntohs(msg->to_ip.ip_port));
        error("TEST FAIL!");
    }

    // Make sure the received message was the right type
    assert(msg->type == type);

    // All good!
}

////////////////////////////////////////////////////////////////////////

//
// Main Server function
//
static void server_main()
{
    setup_ofi_device();

    endpoint_t ep;
    setup_ofi_endpoint(ep, false);

    // Print server addr
    logme("SERVER listening on %s:%d\n",
          inet_ntoa(ep.my_sin.sin_addr),
          ntohs(ep.my_sin.sin_port));

    // Post receives
    post_receives(ep);

    // Broadcast my OFI endpoint IP address+port to all clients
    modex(ep.my_sin);

    // Main server loop
    while (1) {
        // JMS fill me in
        //server_receive_cts();
    }

    teardown_ofi_endpoint(ep);
    teardown_ofi_device();
}

////////////////////////////////////////////////////////////////////////

static void client_hulk_smash(endpoint_t &ep)
{
    logme(id, "Hulk smash!");

    // Tear it all down
    teardown_ofi_endpoint(ep);
    teardown_ofi_device();

    // Recrease
    setup_ofi_device();
    setup_ofi_endpoint(ep, true);
}

static void client_connect(endpoint_t &ep, int server_mcw_rank)
{
    // Fill header of message
    msg_t msg;
    msg_fill_header(msg, MSG_CONNECT, server_mcw_rank);

    // Fill payload of message
    msg.u.connect.client_rdma_key = fi_mr_key(ep.rdma_slab_mr);

    // Send and wait for the message completion
    msg_send_and_wait(ep, msg, server_mcw_rank);
}

static void client_solicit_rdma(endpoint_t &ep, int server_mcw_rank)
{
    // If we're not already connected, connect
    if (!rank_connected(server_mcw_rank)) {
        client_connect(ep, server_mcw_rank);
    }

    // Reset the RDMA slab to known values
    memset(ep.rdma_slab, 17, RDMA_SLAB_SIZE);

    // Ask for an RDMA to a random chunk in the middle of the slab
    size_t offset = (size_t) (RDMA_SLAB_SIZE * drand48());
    size_t len = (size_t) ((RDMA_SLAB_SIZE / 2) * drand48());
    uint8_t *ptr = ep.rdma_slab + offset;
    assert(ptr < ep.rdma_slab + RDMA_SLAB_SIZE);
    assert(ptr + len < ep.rdma_slab + RDMA_SLAB_SIZE);

    // Request some RDMA from the server
    msg_t msg;
    msg_fill_header(msg, MSG_SOLICIT_RDMA, server_mcw_rank);

    // Fill payload of message
    msg.u.solicit_rdma.client_rdma_target_addr = (uint64_t)(uintptr_t) ptr;
    msg.u.solicit_rdma.client_rdma_target_len = (uint64_t) len;

    // Send and wait for the message completion
    msg_send_and_wait(ep, msg, server_mcw_rank);

    // Wait for a message back from the server saying that the RDMA to
    // my slab is done
    cqe_context_t *cqec;
    msg_wait_for_recv(ep, MSG_RDMA_SENT, cqec);

    // Make sure we got a valid reply from the server
    msg_t *received_msg;
    received_msg = (msg_t*) cqec->buffer;
    assert(received_msg->u.rdma_sent.client_rdma_target_addr ==
           (uint64_t)(uintptr_t) ptr);
    assert(received_msg->u.rdma_sent.client_rdma_actual_len <= len);

    // Check that we got what we think we should have gotten
    buffer_pattern_check(ptr, received_msg->u.rdma_sent.client_rdma_actual_len);

    // Repost the receive
    post_receive(ep, cqec->buffer, cqec);
}

static void client_disconnect(endpoint_t &ep, int server_mcw_rank)
{
    // If we're not connected, just return
    if (!rank_connected(server_mcw_rank)) {
        return;
    }

    // Fill header of message
    msg_t msg;
    msg_fill_header(msg, MSG_DISCONNECT, server_mcw_rank);

    // There is no payload for the DISCONNECT message

    // Send and wait for the message
    msg_send_and_wait(ep, msg, server_mcw_rank);
}

//
// Main Client routine
//
static void client_main()
{
    setup_ofi_device();

    // Get all the servers' OFI endpoints IP addresses+ports
    // (contribute {0} for me, because I'm a client)
    struct sockaddr_in sin = {0};
    modex(sin);

    // Create my endpoint
    endpoint_t ep;
    setup_ofi_endpoint(ep, true);

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
            logme("hulk smash!\n");
            client_hulk_smash(ep);
        } else if (chaos < 0.25) {
            logme("client disconnect\n");
            client_disconnect(ep, server_mcw_rank);
        } else {
            logme("solicit RDMA\n");
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
