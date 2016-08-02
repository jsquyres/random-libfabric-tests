#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdarg.h>
#include <assert.h>
#include <sys/epoll.h>

#include <mpi.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

static int comm_rank = 0;
static int comm_size = 0;
static int my_epoll_type = 0;
static char *id = "UNKNOWN";
static int hostname_set = 0;
static char hostname[4096];


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

void wait_for_debugger(void)
{
    printf("%s:%s:MCW %d:PID %d: waiting for debugger attach...\n",
           id, hostname, comm_rank, getpid());
    int i = 0;
    while (i == 0) sleep(5);
}

static const char *addrstr(struct sockaddr_in *sin)
{
    static char foo[BUFSIZ];
    memset(foo, 0, sizeof(foo));
    snprintf(foo, sizeof(foo) - 1, "%s:::%d", inet_ntoa(sin->sin_addr), ntohs(sin->sin_port));
    return foo;
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

static void setup_mpi(void)
{
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    if (0 == comm_rank) {
        id = "SERVER";
        my_epoll_type = 3333;
    } else {
        id = "CLIENT";
        my_epoll_type = 4444;
    }
    int s = sizeof(hostname);
    MPI_Get_processor_name(hostname, &s);
    hostname_set = 1;
}

static void teardown_mpi(void)
{
    MPI_Finalize();
}

////////////////////////////////////////////////////////////////////////

typedef struct fi_device {
    struct fi_info           *info;
    struct fid_fabric        *fabric;
    struct fid_domain        *domain;
    struct fid_av            *av;
    struct fid_eq            *eq;
    int                       eq_fd;
} fi_device_t;

typedef struct fi_conn {
    struct fid_ep            *ep;
    struct fid_cq            *cq;
    int                       cq_fd;
    fi_addr_t                 peer_addr;

    struct fid_mr             bogus_mr;
    struct fid_mr            *send_mr;
    struct fid_mr            *recv_mr;
} fi_conn_t;

static fi_device_t fidev;
static fi_conn_t ficonn;
static int epoll_fd = -1;
static struct sockaddr_in sin;
#define NUM_XDATA 4
static uint32_t *server_data = NULL;
static uint32_t client_data[NUM_XDATA] = { 29, 30, 31, 32 };
static char ofi_node[256] = {0};
static char ofi_service[256] = {0};
static char send_buffer[4096] = {0};
static char recv_buffer[4096] = {0};
static int mr_flags = FI_SEND | FI_RECV | FI_READ | FI_WRITE |
    FI_REMOTE_READ | FI_REMOTE_WRITE;


static void add_epoll_fd(int fd)
{
    // This is a memory leak; I know.  Good enough for a small test.
    struct epoll_event *edt;
    edt = calloc(1, sizeof(*edt));
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
    edt = calloc(1, sizeof(edt));
    assert(edt != NULL);

    edt->events = EPOLLIN;
    edt->data.u32 = my_epoll_type;
    int ret;
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, edt);
    if (ret < 0) {
        error("epoll_ctl failed");
    }
}

static void setup_ofi(const char *node, const char *service,
                      uint64_t flags, uint32_t local_port)
{
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
    ret = fi_getinfo(libfabric_api, node, service, flags,
                     &hints, &fidev.info);
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

    logme("INFO: %s\n", fi_tostr(fidev.info, FI_TYPE_INFO));

     // Am I asking for a specific port?
     struct sockaddr_in *sin;
     sin = (struct sockaddr_in*) fidev.info->src_addr;
     if (local_port > 0) {
         sin->sin_port = htons(local_port);
     } else {
         sin->sin_port = 0;
     }

#if 0
    // Arrgh -- can't get fd's if we use PROGRESS_MANUAL
    fidev.info->domain_attr = calloc(1, sizeof(*(fidev.info->domain_attr)));
    assert(NULL != fidev.info->domain_attr);
    fidev.info->domain_attr->control_progress = FI_PROGRESS_MANUAL;
    fidev.info->domain_attr->data_progress = FI_PROGRESS_MANUAL;
#endif

    // Just use the first info returned
    ret = fi_fabric(fidev.info->fabric_attr, &fidev.fabric, NULL);
    if (0 != ret) {
        error("fi_fabric failed");
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

    ret = fi_domain(fidev.fabric, fidev.info, &fidev.domain, NULL);
    if (0 != ret) {
        error("fi_domain failed");
    }

    // Make an AV
    struct fi_av_attr av_attr = {
        .type = FI_AV_UNSPEC,
        .count = 32
    };
    ret = fi_av_open(fidev.domain, &av_attr, &fidev.av, NULL);
    if (0 != ret) {
        error("fi_av_open failed");
    }

    // Make an endpoint
    ret = fi_endpoint(fidev.domain, fidev.info, &ficonn.ep, NULL);
    if (0 != ret) {
        error("fi_endpoint failed");
    }

    // Bind the EQ to the endpoint
    ret = fi_ep_bind(ficonn.ep, &fidev.eq->fid, 0);
    if (0 != ret) {
        error("fi_ep_bind(eq) failed");
    }

    // Bind the AV to the endpoint
    ret = fi_ep_bind(ficonn.ep, &(fidev.av->fid), 0);
    if (0 != ret) {
        error("fi_ep_bind(av) failed");
    }

    // Make a CQ
    struct fi_cq_attr cq_attr;
    memset(&cq_attr, 0, sizeof(cq_attr));
    cq_attr.format = FI_CQ_FORMAT_MSG;
    cq_attr.wait_obj = FI_WAIT_FD;
    cq_attr.size = 32; // JMS POC
    ret = fi_cq_open(fidev.domain, &cq_attr, &ficonn.cq, NULL);
    if (ret != 0) {
        error("fi_cq_open failed");
    }

    // Bind the CQ TX and RX queues to the EQ
    ret = fi_ep_bind(ficonn.ep, &ficonn.cq->fid, FI_TRANSMIT);
    if (0 != ret) {
        error("fi_ep_bind(cq tx) failed");
    }
    ret = fi_ep_bind(ficonn.ep, &ficonn.cq->fid, FI_RECV);
    if (0 != ret) {
        error("fi_ep_bind(cq rx) failed");
    }

    // Get the fd associated with this CQ
    ret = fi_control(&(ficonn.cq->fid), FI_GETWAIT, &ficonn.cq_fd);
    if (ret != 0) {
        error("fi_control to get cq fq failed");
    }

    // Enable the EP!
    ret = fi_enable(ficonn.ep);
    if (0 != ret) {
        error("fi_enable failed");
    }

    // Register the buffers (must use different keys for each)
    ret = fi_mr_reg(fidev.domain, send_buffer, sizeof(send_buffer),
                    mr_flags, 0, (uintptr_t) send_buffer,
                    0, &ficonn.send_mr, NULL);
    if (ret != 0) {
        error("fi_mr_reg(send) failed\n");
    }
    ret = fi_mr_reg(fidev.domain, recv_buffer, sizeof(recv_buffer),
                    mr_flags, 0, (uintptr_t) recv_buffer,
                    0, &ficonn.recv_mr, NULL);
    if (ret != 0) {
        logme("ERROR: ret=%d, %s\n", ret, fi_strerror(-ret));
        error("fi_mr_reg(recv) failed\n");
    }

    // Make an epoll fd to listen on
    epoll_fd = epoll_create(4096);
    if (epoll_fd < 0) {
        error("epoll_create failed");
    }
    logme("%s made epoll fd: %d\n", id, epoll_fd);

    add_epoll_fd(fidev.eq_fd);
    add_epoll_fd(ficonn.cq_fd);
}

static void teardown_ofi(void)
{
    del_epoll_fd(ficonn.cq_fd);
    del_epoll_fd(fidev.eq_fd);
    close(epoll_fd);

    fi_close(&(ficonn.recv_mr->fid));
    fi_close(&(ficonn.send_mr->fid));
    fi_close(&(ficonn.ep->fid));
    fi_close(&(ficonn.cq->fid));

    fi_close(&(fidev.av->fid));
    fi_close(&(fidev.domain->fid));
    fi_close(&(fidev.eq->fid));
    fi_freeinfo(fidev.info);
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
            logme("%s successfully woke up from epoll! %d events\n", id, nevents);
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

static void wait_for_cq(const char *id, struct fi_cq_msg_entry *cqe_out)
{
    int ret;
    struct fi_cq_msg_entry cqe;

    while (1) {
        wait_for_epoll();

        ret = fi_cq_read(ficonn.cq, &cqe, 1);
        if (-FI_EAGAIN == ret) {
            logme("%s woke up on cq fd, but nothing to read...\n", id);
            continue;
        } else if (-FI_EAVAIL == ret) {
            logme("%s ===== woke up on cq fd, but there's something on the error queue!\n", id);
            struct fi_cq_err_entry cee;
            ret = fi_cq_readerr(ficonn.cq, &cee, 0);
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

        if (NULL != cqe_out) {
            *cqe_out = cqe;
        }
        return;
    }
}

////////////////////////////////////////////////////////////////////////

static void test_lots_of_servers_and_clients_SERVER(void)
{
    int ret;

    logme("SERVER running\n");

    setup_ofi(NULL, NULL, 0, 0);

    // Get the actual address of my EP
    struct sockaddr_in sinout;
    size_t s = sizeof(sinout);
    ret = fi_getname(&(ficonn.ep->fid), &sinout, &s);
    if (0 != ret) {
        error("fi_getname failed");
    }
    sin.sin_family = sinout.sin_family;
    sin.sin_addr = sinout.sin_addr;
    sin.sin_port = sinout.sin_port;

    // Print server addr
    logme("SERVER listening on %s\n", addrstr(&sin));

    // Send our node (IP addr) and service (port) to the client
    snprintf(ofi_node, sizeof(ofi_node) - 1, "%s",
             inet_ntoa(sin.sin_addr));
    snprintf(ofi_service, sizeof(ofi_service) - 1, "%d",
             ntohs(sin.sin_port));
    MPI_Bcast(ofi_node, sizeof(ofi_node) - 1, MPI_CHAR,
             0, MPI_COMM_WORLD);
    MPI_Bcast(ofi_service, sizeof(ofi_service) - 1, MPI_CHAR,
             0, MPI_COMM_WORLD);
    logme("SERVER sent via MPI to clients: %s / %s\n", ofi_node, ofi_service);

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 0: Server posts receives, server broadcasts its address

    // Make a memory registration
    struct fid_mr no_mr;
    struct fid_mr *mr;
    int len = comm_size * sizeof(client_data);
    server_data = calloc(1, len);
    assert(NULL != server_data);

    // Try using no mr, like fi_msg_pingpong...
    memset(&no_mr, 0, sizeof(no_mr));
    mr = &no_mr;

    // Post a recv buffer for the client to send
    len = sizeof(client_data);
    uint8_t *ptr = (uint8_t*) server_data;
    for (int i = 1; i < comm_size; ++i) {
        ret = fi_recv(ficonn.ep, ptr, len, fi_mr_desc(mr), 0, ptr);
        if (ret < 0) {
            logme("fi_recv failed! %d, %s\n", ret, fi_strerror(-ret));
            error("cannot continue");
        }
        logme("SERVER posted receive 0x%p\n", (void*) ptr);
        ptr += len;
    }

    logme("SERVER calling barrier 1\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 1

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 1: Clients fi_send a message

    // Wait for receive completions
    struct fi_cq_msg_entry cqe;
    for (int i = 1; i < comm_size; ++i) {
        // Wait for the receive completion
        wait_for_cq("SERVER", &cqe);

        // Check that we got the expected message
        logme("SERVER got context: 0x%p\n", cqe.op_context);
        uint32_t *p = (uint32_t*) cqe.op_context;
        logme("SERVER received: %d, %d, %d, %d\n",
              p[0],
              p[1],
              p[2],
              p[3]);
        if (memcmp(cqe.op_context, client_data, sizeof(client_data)) != 0) {
	    error("received server message != sent client message");
        }
    }

    logme("SERVER calling barrier 2\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 2

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 2: Clients send addresses, server fi_sends a message

    // Get client addresses
    uint32_t addrport[2];
    struct sockaddr_in peer_addr_sin;
    fi_addr_t *peer_addr_fi;

    peer_addr_fi = calloc(comm_size, sizeof(fi_addr_t));
    assert(peer_addr_fi != NULL);

    for (int i = 1; i < comm_size; ++i) {
        MPI_Recv(addrport, 2, MPI_UINT32_T, i, 101, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        memset(&peer_addr_sin, 0, sizeof(peer_addr_sin));
        peer_addr_sin.sin_family = AF_INET;
        memcpy(&(peer_addr_sin.sin_addr), &addrport[0],
               sizeof(peer_addr_sin.sin_addr));
        peer_addr_sin.sin_port = addrport[1];

        logme("SERVER received  addrport %s:%d from MCW %d\n",
              inet_ntoa(peer_addr_sin.sin_addr), ntohs(peer_addr_sin.sin_port));

        ret = fi_av_insert(fidev.av, &peer_addr_sin, 1, &peer_addr_fi[i], 0, NULL);
        if (ret != 1) {
            error("fi_av_insert failed");
        }

        logme("SERVER sending len of %d to MCW rank %d\n", len, i);
        for (int j = 0; j < NUM_XDATA; ++j) {
            server_data[j] = i;
        }
        len = sizeof(client_data);
        ret = fi_send(ficonn.ep, server_data, len,
                      fi_mr_desc(mr), peer_addr_fi[i], server_data);
        if (ret < 0) {
            logme("fi_Send failed! %d, %s\n", ret, fi_strerror(-ret));
            error("cannot continue");
        }

        // Wait for the send completion
        wait_for_cq("SERVER", NULL);
    }

    logme("SERVER calling barrier 3\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 3

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 3: Clients teardown their endpoints and re-create them;
    // server posts receives

    // Post a recv buffer for the client to send
    len = sizeof(client_data);
    ptr = (uint8_t*) server_data;
    for (int i = 1; i < comm_size; ++i) {
        ret = fi_recv(ficonn.ep, ptr, len, fi_mr_desc(mr), 0, ptr);
        if (ret < 0) {
            logme("fi_recv failed! %d, %s\n", ret, fi_strerror(-ret));
            error("cannot continue");
        }
        logme("SERVER posted receive ptr=0x%p, len=%d\n", (void*) ptr, len);
        ptr += len;
    }

    logme("SERVER calling barrier 4\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 4

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 4: Clients fi_send to server (to show that client->server works)

    for (int i = 1; i < comm_size; ++i) {
        logme("SERVER TIME 4 MPI sending to MCW rank %d for sequencing...\n", i);
        MPI_Send(NULL, 0, MPI_INT, i, 102, MPI_COMM_WORLD);

        logme("SERVER waiting for TIME 4 receive completion from MCW rank %d\n", i);
        wait_for_cq("SERVER", &cqe);

        // Check that we got the expected message
        logme("SERVER got message from client, context: 0x%p\n", cqe.op_context);
        uint32_t *p = (uint32_t*) cqe.op_context;
        for (int j = 0; j < NUM_XDATA; ++j) {
            if (p[j] != i) {
                logme("SERVER received wrong message -- p[%d]=%d, expected %d\n",
                      j, p[j], i);
                error("cannot continue");
            }
        }
    }

    logme("SERVER calling barrier 5\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 5

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 5: Server fi_sends to clients

    for (int i = 1; i < comm_size; ++i) {
        logme("SERVER sending len of %d to MCW rank %d\n", len, i);
        server_data[0] = server_data[1] = server_data[2] = server_data[3] = i + 37;
        ret = fi_send(ficonn.ep, server_data, len,
                      fi_mr_desc(mr), peer_addr_fi[i], server_data);
        if (ret < 0) {
            logme("fi_Send failed! %d, %s\n", ret, fi_strerror(-ret));
            error("cannot continue");
        }

        // Wait for the send completion
        wait_for_cq("SERVER", NULL);
    }

    logme("SERVER calling barrier 6\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 6

    ////////////////////////////////////////////////////////////////////////////////

     logme("SERVER tearing down\n");
    if (mr != &no_mr) {
	    fi_close(&(mr->fid));
    }
    teardown_ofi();
}

//----------------------------------------------------------------------

static void test_lots_of_servers_and_clients_CLIENT(void)
{
    int ret;

    logme("CLIENT running\n");

    // Get the server's node (IP addr) and service (port)
    MPI_Bcast(ofi_node, sizeof(ofi_node) - 1, MPI_CHAR,
              0, MPI_COMM_WORLD);
    MPI_Bcast(ofi_service, sizeof(ofi_service) - 1, MPI_CHAR,
              0, MPI_COMM_WORLD);
    logme("CLIENT received via MPI: %s / %s\n", ofi_node, ofi_service);

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 0: Server posts receives, server broadcasts its address

    logme("CLIENT calling setup_ofi(%s, %s, x)\n", ofi_node, ofi_service);
    setup_ofi(ofi_node, ofi_service, 0, 0);

    // Get the actual address of my EP
    struct sockaddr_in sinout;
    size_t s = sizeof(sinout);
    ret = fi_getname(&(ficonn.ep->fid), &sinout, &s);
    if (0 != ret) {
        error("fi_getname failed");
    }
    sin.sin_family = sinout.sin_family;
    sin.sin_addr = sinout.sin_addr;
    sin.sin_port = sinout.sin_port;
    int client_port = ntohs(sinout.sin_port);

    // Print server addr
    logme("CLIENT listening on %s\n", addrstr(&sin));

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    inet_aton(ofi_node, &sin.sin_addr);
    sin.sin_port = htons(atoi(ofi_service));
    logme("CLIENT translated: %s\n", addrstr(&sin));

    // Print server addr
    logme("CLIENT resolving %s\n", addrstr(&sin));

    // Insert to AV / resolve
    ret = fi_av_insert(fidev.av, &sin, 1, &(ficonn.peer_addr), 0, NULL);
    if (ret != 1) {
        error("fi_av_insert failed");
    }

    logme("CLIENT calling barrier 1\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 1

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 1: Clients fi_send a message

    // Make a memory registration
    struct fid_mr no_mr;
    struct fid_mr *mr;
    void *send_context = (void*) 0x42;
    int len = sizeof(client_data);

    // Try using no mr, like fi_msg_pingpong...
    memset(&no_mr, 0, sizeof(no_mr));
    mr = &no_mr;

    logme("CLIENT sending len of %d\n", len);
    ret = fi_send(ficonn.ep, client_data, len,
                  fi_mr_desc(mr), ficonn.peer_addr, send_context);
    if (ret < 0) {
        logme("fi_Send failed! %d, %s\n", ret, fi_strerror(-ret));
        error("cannot continue");
    }

    // Wait for send completion
    wait_for_cq("CLIENT", NULL);

    logme("CLIENT calling barrier 2\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 2

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 2: Clients send addresses, server fi_sends a message

    // Post a receive
    len = sizeof(client_data);
    ret = fi_recv(ficonn.ep, client_data, len, fi_mr_desc(mr), 0, client_data);
    if (ret < 0) {
        logme("fi_recv failed! %d, %s\n", ret, fi_strerror(-ret));
        error("cannot continue");
    }
    logme("CLIENT posted receive 0x%p\n", (void*) client_data);

    // Send my data to MCW 0
    uint32_t addrport[2];
    memcpy(&(addrport[0]), &(sinout.sin_addr), sizeof(sinout.sin_addr));
    addrport[1] = sinout.sin_port;
    MPI_Send(addrport, 2, MPI_UINT32_T, 0, 101, MPI_COMM_WORLD);

    // Wait for the receive to complete
    wait_for_cq("CLIENT", NULL);

    for (int i = 0; i < NUM_XDATA; ++i) {
        if (client_data[i] != comm_rank) {
            logme("CLIENT got wrong data: got %d, expected %d\n", client_data[i], comm_rank);
            error("CLIENT Cannot continue");
        }
    }

    logme("CLIENT calling barrier 3\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 3

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 3: Clients teardown their endpoints and re-create them;
    // server posts receives

    logme("CLIENT tearing down OFI\n");
    teardown_ofi();

    logme("CLIENT re-creating OFI\n");
    setup_ofi(ofi_node, ofi_service, 0, client_port);

    ret = fi_getname(&(ficonn.ep->fid), &sinout, &s);
    if (0 != ret) {
        error("fi_getname failed");
    }
    // Print server addr
    logme("CLIENT SECOND TIME listening on %s\n", addrstr(&sinout));

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    inet_aton(ofi_node, &sin.sin_addr);
    sin.sin_port = htons(atoi(ofi_service));
    logme("CLIENT translated: %s\n", addrstr(&sin));

    // Print server addr
    logme("CLIENT resolving %s\n", addrstr(&sin));

    // Insert to AV / resolve
    ret = fi_av_insert(fidev.av, &sin, 1, &(ficonn.peer_addr), 0, NULL);
    if (ret != 1) {
        error("fi_av_insert failed");
    }

    logme("CLIENT calling barrier 4\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 4

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 4: Clients fi_send to server (to show that client->server works)

    for (int i = 0; i < NUM_XDATA; ++i) {
        client_data[i] = comm_rank;
    }

    len = sizeof(client_data);
    ret = fi_recv(ficonn.ep, client_data, len, fi_mr_desc(mr), 0, client_data);
    if (ret < 0) {
        logme("fi_recv failed! %d, %s\n", ret, fi_strerror(-ret));
        error("cannot continue");
    }
    logme("CLIENT TIME 4 MCW rank %d posted receive 0x%p\n",
          comm_rank, (void*) client_data);

    logme("CLIENT TIME 4 MCW rank %d MPI receiving from server for sequencing\n",
          comm_rank);
    MPI_Recv(NULL, 0, MPI_INT, 0, 102, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    logme("CLIENT TIME 4 MCW rank %d got message from server\n", comm_rank);

    //wait_for_debugger();

    logme("CLIENT sending client_data filled with my rank (%d) len of %d: first %d\n",
          comm_rank, len, client_data[0]);
    ret = fi_send(ficonn.ep, client_data, len,
                  fi_mr_desc(mr), ficonn.peer_addr, send_context);
    if (ret < 0) {
        logme("CLIENT fi_Send failed! %d, %s\n", ret, fi_strerror(-ret));
        error("cannot continue");
    }
    logme("CLIENT sent client_data filled with my rank (%d) len of %d: first %d\n",
          comm_rank, len, client_data[0]);

    // Wait for send completion
    logme("CLIENT... about to wait for cq...\n");
    wait_for_cq("CLIENT", NULL);

    logme("CLIENT calling barrier 5\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 5

    ////////////////////////////////////////////////////////////////////////////////
    // TIME 5: Server fi_sends to clients

    // Wait for the receive to complete
    wait_for_cq("CLIENT", NULL);

    for (int i = 0; i < NUM_XDATA; ++i) {
        if (client_data[i] != comm_rank + 37) {
            logme("CLIENT got wrong data: got %d, expected %d\n",
                  client_data[i], comm_rank + 37);
            error("CLIENT Cannot continue");
        }
    }

    logme("SERVER calling barrier 6\n");
    MPI_Barrier(MPI_COMM_WORLD); // barrier 6

    ////////////////////////////////////////////////////////////////////////////////

     logme("CLIENT tearing down\n");
    if (mr != &no_mr) {
	    fi_close(&(mr->fid));
    }
    teardown_ofi();
}

static void test_lots_of_servers_and_clients()
{
    if (comm_size != 2) error("Need exactly 2 processes");
    if (0 == comm_rank) {
        test_lots_of_servers_and_clients_SERVER();
    } else {
        test_lots_of_servers_and_clients_CLIENT();
    }

}

////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    setup_mpi();

    test_lots_of_servers_and_clients();

    teardown_mpi();

    return 0;
}
