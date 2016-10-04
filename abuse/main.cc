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

#include <list>

#include <mpi.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include "abuse.h"

using namespace std;


////////////////////////////////////////////////////////////////////////
//
// Global data structures
//

int comm_rank = -1;
int comm_size = -1;
int num_servers = 0;
uint32_t my_epoll_type = 0;
const char *id = "UNKNOWN";
bool hostname_set = false;
char *hostname = NULL;

// -1 = run forever.
// Positive integer = run that many RDMA interactions with each
// client/server pair and then teardown / quit.
int num_interactions = -1;

////////////////////////////////////////////////////////////////////////
//
// Logging functions
//

void logme(const char *msg, ...)
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

void do_error(const char *msg, int line)
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

void wait_for_debugger(void)
{
    printf("%s:%s:MCW %d:PID %d: waiting for debugger attach...\n",
           id, hostname, comm_rank, getpid());
    int i = 0;
    while (i == 0) sleep(5);
}

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

bool i_am_server = false;
bool modex_is_done = false;
modex_data_t *modex_data = NULL;

static void setup_mpi(void)
{
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    int s = MPI_MAX_PROCESSOR_NAME;
    hostname = new char[s];
    assert(hostname);
    MPI_Get_processor_name(hostname, &s);
    hostname_set = true;

    if (comm_size < 2) {
        error("Must run at least 2 MPI processes\n");
    }

    // Get the "first" MPI process on each host.  Do this by
    // allgathering the hostnames.
    char **names = new char*[comm_size];
    assert(names);
    names[0] = new char[comm_size * MPI_MAX_PROCESSOR_NAME];
    assert(names[0]);
    for (int i = 1; i < comm_size; ++i) {
        names[i] = names[i - 1] + MPI_MAX_PROCESSOR_NAME;
    }

    MPI_Allgather(hostname, MPI_MAX_PROCESSOR_NAME, MPI_CHAR,
                  &names[0][0], MPI_MAX_PROCESSOR_NAME, MPI_CHAR,
                  MPI_COMM_WORLD);

    for (int i = 0; i < comm_size; ++i) {
        if (strcmp(names[i], hostname) == 0) {
            if (i == comm_rank) {
                i_am_server = true;
                logme("I AM A SERVER!\n");
            }
            break;
        }
    }

    delete[] names[0];
    delete[] names;

    // Make things (sorta) repeatable
    srand(comm_rank);
    srand48(comm_rank);
}

static void teardown_mpi(void)
{
    MPI_Finalize();

    if (modex_data != NULL) {
        delete[] modex_data;
        modex_data = NULL;
    }

    if (hostname != NULL) {
        delete[] hostname;
        hostname = NULL;
        hostname_set = false;
    }
}

void modex(struct sockaddr_in &sin)
{
    modex_data_t send_data;
    send_data.ip_addr.ip_addr = sin.sin_addr.s_addr;
    send_data.ip_addr.ip_port_be = sin.sin_port;
    send_data.is_server = i_am_server;
    logme("Modex sending: %s:%d\n", inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

    modex_data = new modex_data_t[comm_size];
    assert(modex_data);
    memset(modex_data, 0, sizeof(modex_data_t) * comm_size);

    MPI_Allgather(&send_data, 3, MPI_UINT32_T,
                  modex_data, 3, MPI_UINT32_T, MPI_COMM_WORLD);

    // Make sure that there's at least one client (there will always
    // be at least one client)
    bool found_client = false;
    for (int i = 0; i < comm_size; ++i) {
        if (!modex_data[i].is_server) {
            found_client = true;
        }
    }
    if (!found_client) {
        error("Must run more than one process per server\n");
    }

    modex_is_done = true;
}

////////////////////////////////////////////////////////////////////////
//
// OFI-related data structures
//

device_t fidev;

static const int AV_SIZE = 128;
static const int CQ_SIZE = 8192;
static int epoll_fd = -1;

struct fid_mr no_mr = {0};
int mr_flags = FI_SEND | FI_RECV | FI_READ | FI_WRITE |
    FI_REMOTE_READ | FI_REMOTE_WRITE;

uint64_t cqe_seq = 0;
uint64_t msg_seq = 0;

const size_t DEFAULT_RECEIVE_SIZE = 12000; // JMS semi-arbitrary large-ish number

const size_t RDMA_SLAB_SIZE = 1024 * 1024 * 12;

// These two functions need to be defined this far down because it
// uses data structures and global variables defined just above.
void log_inbound_msg(msg_t *msg, const char *label)
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

void log_outbound_msg(endpoint_t &ep, int mcw_rank, const char *label)
{
    assert(modex_is_done);

    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = modex_data[mcw_rank].ip_addr.ip_addr;
    addr_sin.sin_port = modex_data[mcw_rank].ip_addr.ip_port_be;

    char *src_ip_str = strdup(inet_ntoa(ep.my_sin.sin_addr));
    char *dest_ip_str = strdup(inet_ntoa(addr_sin.sin_addr));
    logme("Sending %s from %s:%d to %s:%d (MCW rank %d)\n",
          label,
          src_ip_str,
          ntohs(ep.my_sin.sin_port),
          dest_ip_str,
          ntohs(addr_sin.sin_port),
          mcw_rank);

    free(dest_ip_str);
    free(src_ip_str);
}

////////////////////////////////////////////////////////////////////////
//
// Epoll utility functions
//

static list<void*> mem_to_free;

static void add_epoll_fd(int fd)
{
    struct epoll_event *edt;
    edt = (struct epoll_event*) calloc(1, sizeof(*edt));
    assert(edt != NULL);
    mem_to_free.push_back((void*) edt);

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
    struct epoll_event *edt;
    edt = (struct epoll_event*) calloc(1, sizeof(edt));
    assert(edt != NULL);
    mem_to_free.push_back((void*) edt);

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

void buffer_pattern_fill(uint8_t *ptr, uint64_t len)
{
    for (uint64_t i = 0; i < len; ++i, ++ptr) {
        *ptr = i % 255;
    }
}

void buffer_pattern_check(uint8_t *ptr, uint64_t len)
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
    if (count > 0) {
        error("Cannot continue\n");
    }
}

////////////////////////////////////////////////////////////////////////
//
// OFI utility functions
//

void setup_ofi_device(void)
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
    // Make sure we specify we want message ordering
    fidev.info->tx_attr->comp_order |= FI_ORDER_SAS;
    fidev.info->rx_attr->comp_order |= FI_ORDER_SAS;
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

    add_epoll_fd(fidev.eq_fd);
}

void setup_ofi_endpoint(endpoint_t &ep)
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

void setup_ofi_rdma_slab(endpoint_t &ep)
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

void teardown_ofi_rdma_slab(endpoint_t &ep)
{
    if (ep.rdma_slab) {
        fi_close(&(ep.rdma_slab_mr->fid));
        delete[] ep.rdma_slab;
    }
}

void teardown_ofi_endpoint(endpoint_t &ep)
{
    fi_close(&(ep.ep->fid));

    if (ep.receive_buffers) {
        delete[] ep.receive_buffers;
    }
    if (ep.cqe_contexts) {
        delete[] ep.cqe_contexts;
    }

    del_epoll_fd(ep.cq_fd);
    fi_close(&(ep.cq->fid));
}

void teardown_ofi_device(void)
{
    del_epoll_fd(fidev.eq_fd);
    close(epoll_fd);

    fi_close(&(fidev.av->fid));
    fi_close(&(fidev.domain->fid));
    fi_close(&(fidev.eq->fid));
    fi_freeinfo(fidev.info);

    list<void*>::iterator it;
    for (it = mem_to_free.begin(); it != mem_to_free.end(); it++) {
        free(*it);
    }
}

void teardown_ofi(endpoint_t &ep)
{
    teardown_ofi_rdma_slab(ep);
    teardown_ofi_endpoint(ep);
    teardown_ofi_device();
}

void wait_for_cq(struct fid_cq *cq, struct fi_cq_msg_entry &cqe_out)
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

void post_receive(endpoint_t &ep, uint8_t *receive_buffer, void *context)
{
    int ret;
    ret = fi_recv(ep.ep, receive_buffer, ep.receive_buffer_size,
                  fi_mr_desc(&no_mr), 0, context);
    if (ret < 0) {
        logme("fi_recv failed! %d, %s\n", ret, fi_strerror(-ret));
        error("cannot continue");
    }
}

void post_receives(endpoint_t &ep)
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

void msg_fill_header(endpoint_t &ep, msg_t *msg, msg_type_t type)
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

cqe_context_t *msg_send(endpoint_t &ep, msg_t *msg, fi_addr_t peer_fi)
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

    return cqec;
}

void msg_verify(endpoint_t &ep, msg_t *msg)
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

int main(int argc, char* argv[])
{
    setup_mpi();
    if (i_am_server) {
        id = "SERVER";
        my_epoll_type = 3333;
        server_main();
        logme("**** Back from server main\n");
    } else {
        id = "CLIENT";
        my_epoll_type = 4444;
        client_main();
        logme("**** Back from client main\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);
    logme("**** Done with final barrier\n");
    teardown_mpi();

    return 0;
}
