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
#include <inttypes.h>
#include <assert.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>


typedef struct fi_device {
    struct fi_info           *info;
    struct fid_fabric        *fabric;
    struct fid_domain        *domain;
    struct fid_av            *av;
    struct fid_eq            *eq;
    int                       eq_fd;
} fi_device_t;

static fi_device_t fidev;


static void test_av()
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
        fprintf(stderr, "cannot fi_getinfo\n");
        exit(1);
    }
    printf("Got info\n");

    int num_devs = 0;
    for (struct fi_info *info = fidev.info;
         NULL != info; info = info->next) {
        ++num_devs;
    }
    if (0 == num_devs) {
        fprintf(stderr, "no fi devices available\n");
        exit(1);
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
        fprintf(stderr, "fi_fabric failed\n");
        exit(1);
    }
    printf("Got fabric\n");

    // Make a domain
    ret = fi_domain(fidev.fabric, fidev.info, &fidev.domain, NULL);
    if (0 != ret) {
        fprintf(stderr, "fi_domain failed\n");
        exit(1);
    }
    printf("Got Domain\n");

    // Make an AV
    struct fi_av_attr av_attr = {
        .type = FI_AV_UNSPEC,
        .count = 32
    };
    ret = fi_av_open(fidev.domain, &av_attr, &fidev.av, NULL);
    if (0 != ret) {
        fprintf(stderr, "fi_av_open failed\n");
        exit(1);
    }
    printf("Got AV\n");

    // Add and remove a bunch of addresses
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_port = htons(5000);

    fi_addr_t addr_fi;
    for (int i = 0; i < 128; ++i) {
        addr_sin.sin_addr.s_addr = 0x08080808;
        ret = fi_av_insert(fidev.av, &addr_sin, 1, &addr_fi, 0, NULL);
        if (ret != 1)  {
            fprintf(stderr, "Failed to fi_av_insert address %d: ret=%d\n", i, ret);
        }
        printf("Added address %d: 0x%" PRIx64 "\n", i, addr_fi);

        ret = fi_av_remove(fidev.av, &addr_fi, 1, 0);
        assert(ret == 0);
    }

    printf("All done\n");
}

////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    test_av();

    return 0;
}
