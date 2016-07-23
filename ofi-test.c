#define WANT_FDS 0
#define WANT_FIXED_PORT 1

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
static char hostname[4096];


#define error(msg) do_error((msg), __LINE__)

static void do_error(const char *msg, int line)
{
    fprintf(stderr, "%s:MCW %d:line %d: %s\n", id, comm_rank, line, msg);
    MPI_Abort(MPI_COMM_WORLD, 1);
    exit(1);
}

static void wait_for_debugger(void)
{
    printf("%s:%s:MCW %d:PID %d: waiting for debugger attach...\n",
           id, hostname, comm_rank, getpid());
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
    int s = sizeof(hostname);
    MPI_Get_processor_name(hostname, &s);
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
    struct fid_eq            *eq;
    int                       eq_fd;
    struct fid_pep           *pep;
} fi_device_t;

typedef struct fi_conn {
    struct fid_ep            *ep;
    struct fid_cq            *cq;
    int                       cq_fd;
    struct fid_mr            bogus_mr;
    struct fid_mr            *send_mr;
    struct fid_mr            *recv_mr;
} fi_conn_t;

static fi_device_t fidev;
static fi_conn_t ficonn;
static int epoll_fd = -1;
static struct sockaddr_in sin;
static int listen_port = 5050;
#define NUM_XDATA 4
static uint32_t server_data[NUM_XDATA] = { 19, 20, 21, 22 };
static uint32_t client_data[NUM_XDATA] = { 29, 30, 31, 32 };
static char ofi_node[256] = {0};
static char ofi_service[256] = {0};
static char send_buffer[4096] = {0};
static char recv_buffer[4096] = {0};


static void setup_ofi(const char *node, const char *service,
                      uint64_t flags)
{
    struct fi_fabric_attr fabric_attr;
    memset(&fabric_attr, 0, sizeof(fabric_attr));
    fabric_attr.prov_name = (char*) "sockets";
    //fabric_attr.prov_name = (char*) "usnic";
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

    printf("INFO: %s\n", fi_tostr(fidev.info, FI_TYPE_INFO));

    // Just use the first info returned
    ret = fi_fabric(fidev.info->fabric_attr, &fidev.fabric, NULL);
    if (0 != ret) {
        error("fi_fabric failed");
    }

    // Make an EQ
    struct fi_eq_attr eq_attr;
    memset(&eq_attr, 0, sizeof(eq_attr));
#if WANT_FDS
    eq_attr.wait_obj = FI_WAIT_FD;
#else
    eq_attr.wait_obj = FI_WAIT_UNSPEC;
#endif
    ret = fi_eq_open(fidev.fabric, &eq_attr, &fidev.eq, NULL);
    if (0 != ret) {
        error("fi_eq failed");
    }

#if WANT_FDS
    // Get the fd associated with this EQ
    ret = fi_control(&(fidev.eq->fid), FI_GETWAIT, &fidev.eq_fd);
    if (ret < 0) {
        error("fi_control to get eq fq failed");
    }
#endif

    ret = fi_domain(fidev.fabric, fidev.info, &fidev.domain, NULL);
    if (0 != ret) {
        error("fi_domain failed");
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
    // Make an EQ
    int ret;
    ret = fi_endpoint(fidev.domain, info, ep, NULL);
    if (0 != ret) {
        error("fi_endpoint failed");
    }

#if WANT_FDS
    // Add the EQ FD to the epoll fd
    static struct epoll_event edt;
    memset(&edt, 0, sizeof(edt));
    edt.events = EPOLLIN;
    edt.data.u32 = 2222;
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fidev.eq_fd, &edt);
    if (ret < 0) {
        error("epoll_ctl failed");
    }
#endif

    // Bind the EP to the EQ
    ret = fi_ep_bind(*ep, &fidev.eq->fid, 0);
    if (0 != ret) {
        error("fi_ep_bind(eq) failed");
    }

    // Make a CQ
    struct fi_cq_attr cq_attr;
    memset(&cq_attr, 0, sizeof(cq_attr));
    cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    cq_attr.wait_obj = FI_WAIT_FD;
    cq_attr.size = 32; // JMS POC
    ret = fi_cq_open(fidev.domain, &cq_attr, &ficonn.cq, NULL);
    if (ret != 0) {
        error("fi_cq_open failed");
    }

    // Bind the CQ TX and RX queues to the EQ
    ret = fi_ep_bind(*ep, &ficonn.cq->fid, FI_TRANSMIT);
    if (0 != ret) {
        error("fi_ep_bind(cq tx) failed");
    }
    ret = fi_ep_bind(*ep, &ficonn.cq->fid, FI_RECV);
    if (0 != ret) {
        error("fi_ep_bind(cq rx) failed");
    }

#if WANT_FDS
    // Get the fd associated with this CQ
    ret = fi_control(&(ficonn.cq->fid), FI_GETWAIT, &ficonn.cq_fd);
    if (ret != 0) {
        error("fi_control to get cq fq failed");
    }
#endif

    // Enable the EP!
    ret = fi_enable(*ep);
    if (0 != ret) {
        error("fi_enable failed");
    }

    // Register the buffers (must use different keys for each)
    ret = fi_mr_reg(fidev.domain, send_buffer, sizeof(send_buffer),
                    FI_SEND, 0, (uintptr_t) send_buffer,
                    0, &ficonn.send_mr, NULL);
    if (ret != 0) {
        error("fi_mr_reg(send) failed\n");
    }
    ret = fi_mr_reg(fidev.domain, recv_buffer, sizeof(recv_buffer),
                    FI_RECV, 0, (uintptr_t) recv_buffer,
                    0, &ficonn.recv_mr, NULL);
    if (ret != 0) {
        printf("ERROR: ret=%d, %s\n", ret, fi_strerror(-ret));
        error("fi_mr_reg(recv) failed\n");
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

    printf("SERVER running\n");

    setup_ofi(NULL, NULL, FI_SOURCE);

#if WANT_FDS
    // Add the EQ FD to the epoll fd
    static struct epoll_event edt;
    memset(&edt, 0, sizeof(edt));
    edt.events = EPOLLIN;
    edt.data.u32 = 2222;
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fidev.eq_fd, &edt);
    if (ret < 0) {
        error("server epoll_ctl failed");
    }
#endif

    // Make a PEP
    ret = fi_passive_ep(fidev.fabric, fidev.info, &fidev.pep, NULL);
    if (0 != ret) {
        error("fi_passive_ep failed");
    }

#if WANT_FIXED_PORT
    size_t ss = sizeof(sin);
    ret = fi_getname(&(fidev.pep->fid), &sin, &ss);
    if (0 != ret) {
        error("fi_setname failed");
    }
    sin.sin_port = htons(listen_port);

    // Bind the PEP to listen on a specific port
    ret = fi_setname(&(fidev.pep->fid), &sin, sizeof(sin));
    if (0 != ret) {
        error("fi_setname failed");
    }
#endif

    // Bind the EQ to the PEP
    ret = fi_pep_bind(fidev.pep, &fidev.eq->fid, 0);
    if (0 != ret) {
        error("fi_pep_bind failed");
    }

    // Listen
    ret = fi_listen(fidev.pep);
    if (0 != ret) {
        error("fi_listen failed");
    }

    // Get the actual address of this PEP
    struct sockaddr_in sinout;
    size_t s = sizeof(sinout);
    ret = fi_getname(&(fidev.pep->fid), &sinout, &s);
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
    int timeout = 10000;
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
            printf("SERVER successfully woke up from epoll! %d events\n", nevents);
            for (int i = 0; i < nevents; ++i) {
                if (events[i].data.u32 != 2222) {
                    error("server unexpected epoll return type");
                }
            }
            // If we got the expected event, then go read from the EQ
            break;
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
        ret = fi_eq_read(fidev.eq, &event, entry, expected_len, 0);
#else
        ret = fi_eq_sread(fidev.eq, &event, entry, expected_len, -1, 0);
#endif
        if (-FI_EAVAIL == ret) {
            printf("server fi_eq_sread failed because there's something in the error queue\n");
            char buffer[2048];
            struct fi_eq_err_entry *err_entry = (struct fi_eq_err_entry*) buffer;
            ret = fi_eq_readerr(fidev.eq, err_entry, 0);
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

    // Silly logistics: setup_ofi_active adds the fd to the epoll set.
    // But we already added it.  So for simplicity, just remove it
    // here so that setup_ofi_active() can re-add it.
#if WANT_FDS
    // Remove the EQ FD from the epoll fd
    ret = epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fidev.eq_fd, &edt);
    if (ret < 0) {
        error("server epoll_ctl DEL failed");
    }
#endif

    // Make an active endpoint
    setup_ofi_active(entry->info, &ficonn.ep);

    // Accept the incoming connection
    ret = fi_accept(ficonn.ep, (void*) server_data, sizeof(server_data));
    if (ret != 0) {
        printf("fi_accept: ret=%d, %s\n", ret, fi_strerror(-ret));
        error("SERVER fi_accept failed\n");
    }

    // Need to read and get a FI_CONNECTED event
    while (1) {
        printf("SERVER waiting for FI_CONNECTED\n");
#if WANT_FDS
        ret = fi_eq_read(fidev.eq, &event, entry, expected_len, 0);
#else
        ret = fi_eq_sread(fidev.eq, &event, entry, expected_len, -1, 0);
#endif
        if (-FI_EAVAIL == ret) {
            printf("server fi_eq_sread failed because there's something in the error queue\n");
            char buffer[2048];
            struct fi_eq_err_entry *err_entry = (struct fi_eq_err_entry*) buffer;
            ret = fi_eq_readerr(fidev.eq, err_entry, 0);
            printf("error code: %d (%s), prov err code: %d (%s)\n", err_entry->err, fi_strerror(err_entry->err), err_entry->prov_errno, fi_strerror(err_entry->prov_errno));
            error("sad panda");
        } else if (-EAGAIN == ret) {
            fprintf(stderr, "SERVER fi_eq_sread fail got -EAGAIN... trying again...\n");
            sleep(1);
            continue;
        } else if (ret < 0) {
            fprintf(stderr, "SERVER fi_eq_sread fail: %s (FI_EAVAIL = %d, -ret = %d)\n", fi_strerror(-ret), FI_EAVAIL, -ret);
            error("SERVER fi_eq_sread failed for some random reason");
        } else if (event != FI_CONNECTED) {
            error("SERVER got some unexpected event");
        }

        printf("SERVER got FI_CONNECTED -- yay!\n");
        break;
    }

    // Post a recv buffer for the client to send
    int msg[4] = { 0 };
    int len = sizeof(msg);
    printf("SERVER receiving len of %d\n", len);

    struct fid_mr no_mr;
    struct fid_mr *mr;
    void *recv_context = (void*) 0x17;
#if 0
    fi_mr_reg(fidev.domain, msg, len, FI_SEND | FI_RECV,
              0, (uint64_t)(uintptr_t) msg, 0, &mr, NULL);
#else
    // Try using no mr, like fi_msg_pingpong...
    memset(&no_mr, 0, sizeof(no_mr));
    mr = &no_mr;
#endif
    ret = fi_recv(ficonn.ep, msg, len,
                  fi_mr_desc(mr), 0, recv_context);
    if (ret < 0) {
        printf("fi_recv failed! %d, %s\n", ret, fi_strerror(-ret));
        MPI_Abort(MPI_COMM_WORLD, 37);
    }

    sleep(1);
    printf("SERVER posted receive -- waiting for client to send\n");
    MPI_Barrier(MPI_COMM_WORLD);

    // Wait for receive completion
    struct fi_cq_entry cqe;
    while (1) {
        ret = fi_cq_sread(ficonn.cq, &cqe, 1, 0, -1);
        if (cqe.op_context == recv_context) {
            printf("SERVER receive completed\n");
            break;
        } else {
            printf("SERVER got some other completion... continuing\n");
        }
    }

    printf("SERVER finished -- waiting for client before teardown\n");
    MPI_Barrier(MPI_COMM_WORLD);

    printf("SERVER tearing down\n");
    fi_close(&(mr->fid));
    teardown_ofi();
}

//----------------------------------------------------------------------

static void test_connect_with_accept_blocking_on_eq_fq_CLIENT(void)
{
    int ret;

    printf("CLIENT running\n");

    // Get the server's node (IP addr) and service (port)
    MPI_Recv(ofi_node, sizeof(ofi_node) - 1, MPI_CHAR,
             0, 101, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(ofi_service, sizeof(ofi_service) - 1, MPI_CHAR,
             0, 102, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("CLIENT received via MPI: %s / %s\n", ofi_node, ofi_service);

    //setup_ofi(ofi_node, ofi_service);
    setup_ofi(NULL, NULL, 0);

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    inet_aton(ofi_node, &sin.sin_addr);
    sin.sin_port = htons(atoi(ofi_service));
    printf("CLIENT translated: %s\n", addrstr(&sin));

    setup_ofi_active(fidev.info, &ficonn.ep);

    // Print server addr
    printf("CLIENT connecting to %s\n", addrstr(&sin));

    // Connect!
    printf("Client connecting...\n");
    ret = fi_connect(ficonn.ep,
                     //fidev.info->dest_addr,
                     &sin,
                     (void*) client_data, sizeof(client_data));
    if (ret < 0) {
        error("fi_connect failed");
    }

#if WANT_FDS
    // Now wait for the listen to complete
    int nevents;
    #define NEVENTS 32
    struct epoll_event events[NEVENTS];
    int timeout = 10000;
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
            printf("CLIENT successfully woke up from epoll! %d events\n", nevents);
            for (int i = 0; i < nevents; ++i) {
                if (events[i].data.u32 != 2222) {
                    error("CLIENT unexpected epoll return type");
                }
            }
            // If we got the expected event, then go read from the EQ
            break;
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
        printf("CLIENT waiting for FI_CONNECTED\n");
#if WANT_FDS
        ret = fi_eq_read(fidev.eq, &event, entry, expected_len, 0);
#else
        ret = fi_eq_sread(fidev.eq, &event, entry, expected_len, -1, 0);
#endif
        if (-FI_EAVAIL == ret) {
            fprintf(stderr, "client fi_eq_sread failed because there's something in the error queue\n");
            char buffer[2048];
            struct fi_eq_err_entry *err_entry = (struct fi_eq_err_entry*) buffer;
            ret = fi_eq_readerr(fidev.eq, err_entry, 0);
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

    printf("CLIENT connecting -- waiting for server before sending\n");
    MPI_Barrier(MPI_COMM_WORLD);

    sleep(1);
    int msg[4] = { 99, 100, 101, 102 };
    int len = sizeof(msg);
    printf("CLIENT sending len of %d\n", len);

    struct fid_mr no_mr;
    struct fid_mr *mr;
    void *send_context = (void*) 0x42;
#if 0
    fi_mr_reg(fidev.domain, msg, len, FI_SEND | FI_RECV,
              0, (uint64_t)(uintptr_t) msg, 0, &mr, NULL);
#else
    // Try using no mr, like fi_msg_pingpong...
    memset(&no_mr, 0, sizeof(no_mr));
    mr = &no_mr;
#endif
    ret = fi_send(ficonn.ep, msg, len,
                  fi_mr_desc(mr), 0, send_context);
    if (ret < 0) {
        printf("fi_Send failed! %d, %s\n", ret, fi_strerror(-ret));
        MPI_Abort(MPI_COMM_WORLD, 37);
    }

    // Wait for send completion
    struct fi_cq_entry cqe;
    while (1) {
        ret = fi_cq_sread(ficonn.cq, &cqe, 1, 0, -1);
        if (cqe.op_context == send_context) {
            printf("CLIENT send completed\n");
            break;
        } else {
            printf("CLIENT got some other completion... continuing\n");
        }
    }

    printf("CLIENT sent -- waiting for server before teardown\n");
    MPI_Barrier(MPI_COMM_WORLD);

    printf("CLIENT tearing down\n");
    fi_close(&(mr->fid));
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
