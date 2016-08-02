#define WANT_FDS 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#if WANT_FDS
#include <sys/epoll.h>
#endif

#include <mpi.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

static int comm_rank;
static int comm_size;
static char *id = "UNKNOWN";


#if WANT_FDS
typedef enum {
    EVENT_I_AM_EXPECTED
} event_type;

typedef struct {
    struct epoll_event event;
    event_type et;
} epoll_context_t;
#endif

#define error(msg) do_error((msg), __LINE__)

static void do_error(const char *msg, int line)
{
    fprintf(stderr, "%s:MCW %d:line %d: %s\n", id, comm_rank, line, msg);
    MPI_Abort(MPI_COMM_WORLD, 1);
    exit(1);
}

void wait_for_debugger(void)
{
    printf("%s:MCW %d:PID %d: waiting for debugger attach...\n",
           id, comm_rank, getpid());
    int i = 0;
    while (i == 0) sleep(5);
}

static const char *addrstr(struct sockaddr_in *sin)
{
    static char namebuf[BUFSIZ];
    static char servbuf[BUFSIZ];

    memset(namebuf, 0, sizeof(namebuf));
    memset(servbuf, 0, sizeof(servbuf));
    socklen_t len = sizeof(*sin);
    getnameinfo((struct sockaddr*) sin, len, namebuf, BUFSIZ,
                servbuf, BUFSIZ,
                NI_NUMERICHOST | NI_NUMERICSERV);

    static char foo[BUFSIZ];
    memset(foo, 0, sizeof(foo));
    snprintf(foo, sizeof(foo) - 1, "%s:::%s", namebuf, servbuf);
    return foo;

    return inet_ntoa(sin->sin_addr);
}

////////////////////////////////////////////////////////////////////////

static void setup_mpi(void)
{
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    if (0 == comm_rank) {
        id = "SERVER";
    } else if (1 == comm_rank) {
        id = "CLIENT";
    }
}

static void teardown_mpi(void)
{
    MPI_Finalize();
}

////////////////////////////////////////////////////////////////////////

typedef struct fistuff {
    struct fi_info           *info;
    struct fid_fabric        *fabric;
    struct fid_domain        *domain;
    struct fid_eq            *eq;
    int                       eq_fd;
    struct fid_pep           *pep;
    struct fid_cq            *cq;
    int                       cq_fd;
    struct fid_mr            *send_mr;
    struct fid_mr            *recv_mr;
} fistuff_t;

typedef struct {
    struct fid_ep            *ep;
    fi_addr_t                 addr;
} clientstuff_t;

typedef struct {
    struct fid_ep            *ep;
} serverstuff_t;

static fistuff_t fistuff;
static int epoll_fd = -1;
static struct sockaddr_in sin;
#define NUM_CONN_DATA 4
static uint32_t server_data[NUM_CONN_DATA] = { 19, 20, 21, 22 };
static uint32_t client_data[NUM_CONN_DATA] = { 29, 30, 31, 32 };
static char ofi_node[256] = {0};
static char ofi_service[256] = {0};
static char send_buffer[4096] = {0};
static char recv_buffer[4096] = {0};


static void setup_ofi(const char *node, const char *service)
{
    struct fi_fabric_attr fabric_attr;
    memset(&fabric_attr, 0, sizeof(fabric_attr));
    //fabric_attr.prov_name = (char*) "sockets";
    fabric_attr.prov_name = (char*) "usnic";
    struct fi_ep_attr ep_attr;
    memset(&ep_attr, 0, sizeof(ep_attr));
    ep_attr.type = FI_EP_MSG;

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
    ret = fi_getinfo(libfabric_api, node, service, 0,
                     &hints, &fistuff.info);
    if (0 != ret) {
        error("cannot fi_getinfo");
    }

    int num_devs = 0;
    for (struct fi_info *info = fistuff.info;
         NULL != info; info = info->next) {
        ++num_devs;
    }
    if (0 == num_devs) {
        error("no fi devices available");
    }

    //printf("INFO: %s\n", fi_tostr(fistuff.info, FI_TYPE_INFO));

    // Just use the first info returned
    ret = fi_fabric(fistuff.info->fabric_attr, &fistuff.fabric, NULL);
    if (0 != ret) {
        error("fi_fabric failed");
    }

    ret = fi_domain(fistuff.fabric, fistuff.info, &fistuff.domain, NULL);
    if (0 != ret) {
        error("fi_domain failed");
    }

    // Make an EQ
    struct fi_eq_attr eq_attr;
    memset(&eq_attr, 0, sizeof(eq_attr));
    eq_attr.wait_obj = FI_WAIT_FD;
    ret = fi_eq_open(fistuff.fabric, &eq_attr, &fistuff.eq, NULL);
    if (0 != ret) {
        error("fi_eq failed");
    }

#if WANT_FDS
    // Get the fd associated with this EQ
    ret = fi_control(&(fistuff.eq->fid), FI_GETWAIT, &fistuff.eq_fd);
    if (ret < 0) {
        error("fi_control to get eq fq failed");
    }
#endif

    // Make a CQ
    struct fi_cq_attr cq_attr;
    memset(&cq_attr, 0, sizeof(cq_attr));
    cq_attr.format = FI_CQ_FORMAT_CONTEXT; // JMS used to be MSG
    cq_attr.wait_obj = FI_WAIT_FD;
    cq_attr.size = 32; // JMS POC
    ret = fi_cq_open(fistuff.domain, &cq_attr, &fistuff.cq, NULL);
    if (ret != 0) {
        error("fi_cq_open failed");
    }

#if WANT_FDS && 0
    // Get the fd associated with this CQ
    ret = fi_control(&(fistuff.cq->fid), FI_GETWAIT, &fistuff.cq_fd);
    if (ret != 0) {
        error("fi_control to get cq fq failed");
    }
#endif

    // Register the buffers (must use different keys for each)
    ret = fi_mr_reg(fistuff.domain, send_buffer, sizeof(send_buffer),
                    FI_SEND, 0, (uintptr_t) send_buffer,
                    0, &fistuff.send_mr, NULL);
    if (ret != 0) {
        error("fi_mr_reg(send) failed\n");
    }
    ret = fi_mr_reg(fistuff.domain, recv_buffer, sizeof(recv_buffer),
                    FI_RECV, 0, (uintptr_t) recv_buffer,
                    0, &fistuff.recv_mr, NULL);
    if (ret != 0) {
        printf("ERROR: ret=%d, %s\n", ret, fi_strerror(-ret));
        error("fi_mr_reg(recv) failed\n");
    }

#if WANT_FDS
    // Make an epoll fd to listen on
    epoll_fd = epoll_create(4096);
    if (epoll_fd < 0) {
        error("epoll_create failed");
    }
#endif
}

static void setup_ofi_active(struct fi_info *info,
                             struct fid_ep **ep)
{
    int ret;
    ret = fi_endpoint(fistuff.domain, info, ep, NULL);
    if (0 != ret) {
        error("fi_endpoint failed");
    }

#if WANT_FDS
    // Add the EQ FD to the epoll fd
    epoll_context_t eq_fd_epoll_context;
    memset(&eq_fd_epoll_context, 0, sizeof(eq_fd_epoll_context));
    eq_fd_epoll_context.et = EVENT_I_AM_EXPECTED;
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fistuff.eq_fd,
                    (void*) &eq_fd_epoll_context);
    if (ret < 0) {
        error("epoll_ctl failed");
    }
#endif

    // Bind the EQ to the endpoint
    ret = fi_ep_bind(*ep, &fistuff.eq->fid, 0);
    if (0 != ret) {
        error("fi_ep_bind(eq) failed");
    }

    // Bind the TX and RX queues to the EQ
    ret = fi_ep_bind(*ep, &fistuff.cq->fid, FI_TRANSMIT);
    if (0 != ret) {
        error("fi_ep_bind(cq tx) failed");
    }
    ret = fi_ep_bind(*ep, &fistuff.cq->fid, FI_RECV);
    if (0 != ret) {
        error("fi_ep_bind(cq rx) failed");
    }

    // Enable!
    ret = fi_enable(*ep);
    if (0 != ret) {
        error("fi_enable failed");
    }
}

static void teardown_ofi(void)
{
    // JMS Fill me in
}

////////////////////////////////////////////////////////////////////////

static void test_connect_with_accept_blocking_on_eq_fq_SERVER(void)
{
    int ret;
    static serverstuff_t server;

    printf("SERVER running\n");

    setup_ofi(NULL, NULL);

#if WANT_FDS
    // Add the EQ FD to the epoll fd
    epoll_context_t eq_fd_epoll_context;
    memset(&eq_fd_epoll_context, 0, sizeof(eq_fd_epoll_context));
    eq_fd_epoll_context.et = EVENT_I_AM_EXPECTED;
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fistuff.eq_fd,
                    (void*) &eq_fd_epoll_context);
    if (ret < 0) {
        error("server epoll_ctl failed");
    }
#endif

    // Make a PEP
    ret = fi_passive_ep(fistuff.fabric, fistuff.info, &fistuff.pep, NULL);
    if (0 != ret) {
        error("fi_passive_ep failed");
    }

    // Bind the EQ to the PEP
    ret = fi_pep_bind(fistuff.pep, &fistuff.eq->fid, 0);
    if (0 != ret) {
        error("fi_pep_bind failed");
    }

    // Listen
    ret = fi_listen(fistuff.pep);
    if (0 != ret) {
        error("fi_listen failed");
    }

    // Get the actual address of this PEP
    struct sockaddr_in sinout;
    size_t s = sizeof(sinout);
    ret = fi_getname(&(fistuff.pep->fid), &sinout, &s);
    if (0 != ret) {
        error("fi_setname failed");
    }
    sin.sin_family = sinout.sin_family;
    sin.sin_addr = sinout.sin_addr;
    sin.sin_port = sinout.sin_port;

    // Print server addr
    printf("SERVER listening on %s\n", addrstr(&sin));

    // Send our node (IP addr) and service (port) to the client
    snprintf(ofi_node, sizeof(ofi_node) - 1, "%s",
             inet_ntoa(sin.sin_addr));
    snprintf(ofi_service, sizeof(ofi_service) - 1, "%d",
             ntohs(sin.sin_port));
    MPI_Send(ofi_node, sizeof(ofi_node) - 1, MPI_CHAR,
             1, 101, MPI_COMM_WORLD);
    MPI_Send(ofi_service, sizeof(ofi_service) - 1, MPI_CHAR,
             1, 102, MPI_COMM_WORLD);
    printf("SERVER sent via MPI to client: %s / %s\n", ofi_node, ofi_service);

#if WANT_FDS
    // Now wait for the listen to complete
    int nevents;
    #define NEVENTS 32
    struct epoll_event events[NEVENTS];
    int timeout = -1;
    while (1) {
        printf("SERVER blocking on epoll\n");
        nevents = epoll_wait(epoll_fd, events, NEVENTS, timeout);
        if (nevents < 0) {
            if (errno != EINTR) {
                error("server epoll wait failed");
            } else {
                continue;
            }
        } else {
            printf("SERVER successfully woke up from epoll!\n");
            for (int i = 0; i < nevents; ++i) {
                epoll_context_t *ptr;
                ptr = (epoll_context_t*) events[i].data.ptr;
                if (ptr->et != EVENT_I_AM_EXPECTED) {
                    error("server unexpected epoll return type");
                }
            }
        }
    }
#endif

    // Wait for the FI_CONNREQ event
    uint32_t event;
    uint8_t *entry_buffer;
    size_t expected_len = sizeof(struct fi_eq_cm_entry) +
        sizeof(client_data);
    entry_buffer = (uint8_t*) calloc(1, expected_len);
    if (NULL == entry_buffer) {
        error("calloc failed");
    }
    struct fi_eq_cm_entry *entry = (struct fi_eq_cm_entry*) entry_buffer;

    while (1) {
        printf("SERVER waiting for FI_CONNREQ\n");
#if WANT_FDS
        ret = fi_eq_read(fistuff.eq, &event, entry, expected_len, 0);
#else
        ret = fi_eq_sread(fistuff.eq, &event, entry, expected_len, -1, 0);
#endif
        if (-FI_EAVAIL == ret) {
            printf("server fi_eq_sread failed because there's something in the error queue\n");
            char buffer[2048];
            struct fi_eq_err_entry *err_entry = (struct fi_eq_err_entry*) buffer;
            ret = fi_eq_readerr(fistuff.eq, err_entry, 0);
            printf("error code: %d (%s), prov err code: %d (%s)\n", err_entry->err, fi_strerror(err_entry->err), err_entry->prov_errno, fi_strerror(err_entry->prov_errno));
            error("sad panda");
        } else if (-EAGAIN == ret) {
            fprintf(stderr, "SERVER fi_eq_sread fail got -EAGAIN... trying again...\n");
            sleep(1);
            continue;
        } else if (ret < 0) {
            fprintf(stderr, "SERVER fi_eq_sread fail: %s (FI_EAVAIL = %d, -ret = %d)\n", fi_strerror(-ret), FI_EAVAIL, -ret);
            error("SERVER fi_eq_sread failed for some random reason");
        } else if (event != FI_CONNREQ) {
            error("SERVER got some unexpected event");
        } else if (ret != expected_len) {
            error("SERVER got wrong length back from fi_eq_sread");
        }

        uint32_t *d = (uint32_t*) entry->data;
        for (int i = 0; i < (sizeof(client_data) / sizeof(uint32_t)); ++i) {
            if (d[i] != client_data[i]) {
                printf("SERVER got wrong CM client data: d[%d]=%d, should be %d\n",
                       i, d[i], client_data[i]);
            }
        }

        printf("SERVER got FI_CONNREQ, correct size, and correct data -- yay!\n");
        break;
    }

    // Make an active endpoint
    setup_ofi_active(entry->info, &server.ep);

    // Accept the incoming connection
    ret = fi_accept(server.ep, (void*) server_data, sizeof(server_data));
    if (ret != 0) {
        printf("fi_accept: ret=%d, %s\n", ret, fi_strerror(-ret));
        error("SERVER fi_accept failed\n");
    }

    printf("SERVER finished -- waiting for client before teardown\n");
    MPI_Barrier(MPI_COMM_WORLD);
    printf("SERVER tearing down\n");
    teardown_ofi();
}

//----------------------------------------------------------------------

static void test_connect_with_accept_blocking_on_eq_fq_CLIENT(void)
{
    int ret;
    static clientstuff_t client;

    printf("CLIENT running\n");

    // Get the server's node (IP addr) and service (port)
    MPI_Recv(ofi_node, sizeof(ofi_node) - 1, MPI_CHAR,
             0, 101, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(ofi_service, sizeof(ofi_service) - 1, MPI_CHAR,
             0, 102, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("CLIENT received via MPI: %s / %s\n", ofi_node, ofi_service);

    setup_ofi(ofi_node, ofi_service);

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    inet_aton(ofi_node, &sin.sin_addr);
    sin.sin_port = htons(atoi(ofi_service));
    printf("CLIENT translated: %s\n", addrstr(&sin));

    setup_ofi_active(fistuff.info, &client.ep);

    // Print server addr
    printf("CLIENT connecting to %s\n", addrstr(&sin));

    // Connect!
    printf("Client connecting...\n");
    ret = fi_connect(client.ep, fistuff.info->dest_addr,
                     (void*) client_data, sizeof(client_data));
    if (ret < 0) {
        error("fi_connect failed");
    }

#if WANT_FDS
    // Now wait for the listen to complete
    int nevents;
    #define NEVENTS 32
    struct epoll_event events[NEVENTS];
    int timeout = -1;
    while (1) {
        printf("CLIENT blocking on epoll\n");
        nevents = epoll_wait(epoll_fd, events, NEVENTS, timeout);
        if (nevents < 0) {
            if (errno != EINTR) {
                error("client epoll wait failed");
            } else {
                continue;
            }
        } else {
            printf("CLIENT successfully woke up from epoll!\n");
            for (int i = 0; i < nevents; ++i) {
                epoll_context_t *ptr;
                ptr = (epoll_context_t*) events[i].data.ptr;
                if (ptr->et != EVENT_I_AM_EXPECTED) {
                    error("CLIENT unexpected epoll return type");
                }
            }
        }
    }
#endif

    // Wait for FI_CONNECTED event
    uint32_t event;
    uint8_t *entry_buffer;
    size_t expected_len = sizeof(struct fi_eq_cm_entry) +
        sizeof(client_data);
    entry_buffer = (uint8_t*) calloc(1, expected_len);
    if (NULL == entry_buffer) {
        error("calloc failed");
    }
    struct fi_eq_cm_entry *entry = (struct fi_eq_cm_entry*) entry_buffer;

    while (1) {
        printf("CLIENT waiting for FI_CONNECT\n");
#if WANT_FDS
        ret = fi_eq_read(fistuff.eq, &event, entry, expected_len, 0);
#else
        ret = fi_eq_sread(fistuff.eq, &event, entry, expected_len, -1, 0);
#endif
        if (-FI_EAVAIL == ret) {
            fprintf(stderr, "client fi_eq_sread failed because there's something in the error queue\n");
            char buffer[2048];
            struct fi_eq_err_entry *err_entry = (struct fi_eq_err_entry*) buffer;
            ret = fi_eq_readerr(fistuff.eq, err_entry, 0);
            fprintf(stderr, "error code: %d (%s), prov err code: %d (%s)\n", err_entry->err, fi_strerror(err_entry->err), err_entry->prov_errno, fi_strerror(err_entry->prov_errno));
            error("sad panda");
        } else if (ret == -EAGAIN) {
            fprintf(stderr, "CLIENT fi_eq_sread fail got -EAGAIN... trying again...\n");
            sleep(1);
            continue;
        } else if (ret < 0) {
            fprintf(stderr, "SERVER fi_eq_sread fail: %s, ret = %d)\n", fi_strerror(-ret), ret);
            error("client fi_eq_sread failed for some random reason");
        } else if (event != FI_CONNECTED) {
            error("client got some unexpected event");
        } else if (ret != expected_len) {
            error("client got wrong length back from fi_eq_sread");
        }

        uint32_t *d = (uint32_t*) entry->data;
        for (int i = 0; i < (sizeof(server_data) / sizeof(uint32_t)); ++i) {
            if (d[i] != server_data[i]) {
                printf("CLIENT got wrong CM client data: d[%d]=%d, should be %d\n",
                       i, d[i], server_data[i]);
            }
        }

        printf("client got FI_CONNECTED, correct size, and correct data -- yay!\n");
        break;
    }

    printf("CLIENT finished -- waiting for server before teardown\n");
    MPI_Barrier(MPI_COMM_WORLD);
    printf("CLIENT tearing down\n");
    teardown_ofi();
}

static void test_connect_with_accept_blocking_on_eq_fq(void)
{
    if (comm_size < 0) error("need exactly 2 MPI procs");
    if (0 == comm_rank) {
        test_connect_with_accept_blocking_on_eq_fq_SERVER();
    } else {
        test_connect_with_accept_blocking_on_eq_fq_CLIENT();
    }

}

////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    setup_mpi();

    test_connect_with_accept_blocking_on_eq_fq();

    teardown_mpi();

    return 0;
}
